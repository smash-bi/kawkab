package kawkab.fs.core;

import kawkab.fs.api.Record;
import kawkab.fs.commons.Commons;
import kawkab.fs.commons.Configuration;
import kawkab.fs.commons.FixedLenRecordUtils;
import kawkab.fs.core.exceptions.*;
import kawkab.fs.core.index.poh.PostOrderHeapIndex;
import kawkab.fs.core.timerqueue.DeferredWorkReceiver;
import kawkab.fs.core.timerqueue.TimerQueue;
import kawkab.fs.core.timerqueue.TimerQueueItem;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public final class Inode implements DeferredWorkReceiver<DataSegment> {
	//FIXME: fileSize can be internally staled. This is because append() is a two step process.
	// The FileHandle first calls append and then calls updateSize to update the fileSize. Ideally,
	// We should update the fileSize immediately after adding a new block. The fileSize is not being
	// updated immediately to allow concurrent reading and writing to the same dataBlock.

	private long inumber;
	private AtomicLong fileSize = new AtomicLong(0);
	private int recordSize; //Temporarily set to 1 until we implement reading/writing records
	private PostOrderHeapIndex index;

	private volatile TimerQueueItem<DataSegment> acquiredSeg;

	private static final Cache cache = Cache.instance();
	private static final Clock clock = Clock.instance();
	private static final LocalStoreManager localStore = LocalStoreManager.instance();
	private static final Configuration conf = Configuration.instance();
	private TimerQueue timerQ;

	private boolean isInited;
	private boolean isLoaded;
	private int recsPerSeg;

	private static final int bufferTimeOffsetMs = 5;

	public static final long MAXFILESIZE = conf.maxFileSizeBytes;

	protected Inode(long inumber, int recordSize) {
		this.inumber = inumber;
		this.recordSize = recordSize;

		if (recordSize >= 1) {
			System.out.printf("[I] Initializing inode: %d, recSize=%d\n", inumber, recordSize);
		}
	}

	/**
	 * Prepare after opening the file.
	 * This function should be called after the inode has been loaded from the file or the remote node or the global store.
	 */
	synchronized void prepare(TimerQueue tq) {
		if (isInited)
			return;
		isInited = true;

		System.out.printf("[I] Initializing index for inode: %d, recSize=%d\n", inumber, recordSize);

		timerQ = tq;

		assert recordSize >= 1;
		recsPerSeg = conf.segmentSizeBytes / recordSize;
		if (recordSize > 1)
			index = new PostOrderHeapIndex(inumber, conf.indexNodeSizeBytes, conf.nodesPerBlockPOH, conf.percentIndexEntriesPerNode, cache, timerQ);



		/*try {
			index.loadAndInit(indexLength(fileSize.get()));
		} catch (IOException | KawkabException e) {
			e.printStackTrace();
		}*/
	}

	synchronized void loadLastBlock() throws KawkabException, IOException {
		// Pre-fetching the last block to warm up for writes
		long fs = fileSize.get();

		System.out.printf("[I] Should load the last block? %b, fs=%d, recsPerSeg*recSize=%d\n",
				fs%(recsPerSeg*recordSize)!=0, fs, recsPerSeg*recordSize);

		if (fs % (recsPerSeg*recordSize) != 0) {
			BlockID curSegId = getByFileOffset(fs);

			System.out.printf("[I] Loading the last seg %s on file open\n", curSegId );

			cache.acquireBlock(curSegId).loadBlock();
			cache.releaseBlock(curSegId);
		}
	}

	/**
	 * Returns the ID of the segment that contains the given offset in file
	 * @throws IOException
	 */
	private BlockID getByFileOffset(long offsetInFile) {
		long segmentInFile = FixedLenRecordUtils.segmentInFile(offsetInFile, recordSize);
		int segmentInBlock = FixedLenRecordUtils.segmentInBlock(segmentInFile);
		long blockInFile = FixedLenRecordUtils.blockInFile(segmentInFile);

		return new DataSegmentID(inumber, blockInFile, segmentInBlock, recordSize);
	}

	private BlockID idBySegInFile(long segInFile) {
		int segmentInBlock = FixedLenRecordUtils.segmentInBlock(segInFile);
		long blockInFile = FixedLenRecordUtils.blockInFile(segInFile);

		return new DataSegmentID(inumber, blockInFile, segmentInBlock, recordSize);
	}

	/**
	 * Performs the read operation. Single writer and multiple readers are allowed to write and read concurrently.
	 *
	 * @return
	 * @throws IllegalArgumentException
	 * @throws InvalidFileOffsetException
	 * @throws IOException
	 * @throws KawkabException
	 */
	public boolean read(final Record dstRecord, final long key) throws
			RecordNotFoundException, IOException, KawkabException {

		assert dstRecord.size() == recordSize;

		long offsetInFile = index.findHighest(key, indexLength(fileSize.get()));

		if (offsetInFile == -1)
			return  false;

		//System.out.println("  Read at offset: " + offsetInFile);
		BlockID curSegId = getByFileOffset(offsetInFile);

		//System.out.println("Reading block at offset " + offsetInFile + ": " + curBlkUuid.key);

		DataSegment curSegment = null;
		try {
			curSegment = (DataSegment)cache.acquireBlock(curSegId);
			curSegment.loadBlock(); //The segment data might not be loaded when we get from the cache
			int bytesRead = curSegment.read(dstRecord.copyInDstBuffer(), offsetInFile);

			assert bytesRead == recordSize;
		} finally {
			if (curSegment != null) {
				cache.releaseBlock(curSegment.id());
			}
		}

		return true;
	}

	/**
	 * @return
	 * @throws IllegalArgumentException
	 * @throws InvalidFileOffsetException
	 * @throws IOException
	 * @throws KawkabException
	 */
	public boolean readRecordN(final Record dstRecord, final long recNum) throws IOException, KawkabException {
		assert recNum > 0;
		assert dstRecord.size() == recordSize;

		long offsetInFile = (recNum-1) * recordSize;
		long fileSize = this.fileSize.get();

		assert offsetInFile >= 0 : String.format("Invalid offsetInFile %d, recNum %d", offsetInFile, recNum);

		if (offsetInFile+recordSize > fileSize)
			throw new KawkabException(String.format("Record number %d is out of range of the file. OffsetInFile=%d, recordSize=%d, recsInFile=%d",
					recNum, offsetInFile, recordSize, fileSize/recordSize));

		if (dstRecord.size() != recordSize)
			throw new KawkabException(String.format("The record size (%d bytes) does not match with the file's record size (%d bytes)",
					dstRecord.size(), recordSize));

		//System.out.println("  Read at offset: " + offsetInFile);
		BlockID curSegId = getByFileOffset(offsetInFile);

		//System.out.println("Reading block at offset " + offsetInFile + ": " + curBlkUuid.key);

		DataSegment curSegment = null;
		try {
			curSegment = (DataSegment)cache.acquireBlock(curSegId);
			curSegment.loadBlock(); //The segment data might not be loaded when we get from the cache
			int bytesRead = curSegment.read(dstRecord.copyInDstBuffer(), offsetInFile);

			assert bytesRead == recordSize;
		} finally {
			if (curSegment != null) {
				cache.releaseBlock(curSegment.id());
			}
		}

		return true;
	}

	public List<Record> readAll(long minTS, long maxTS, Record recFactory) throws IOException, KawkabException {
		assert recFactory.size() == recordSize;

		List<long[]> offsets = index.findAll(minTS, maxTS, indexLength(fileSize.get())); //Get the offsets

		if (offsets == null)
			return null;

		//System.out.println(offsets.size());

		int length = offsets.size();
		List<Record> results = new ArrayList<>(); //The lists contains offsets to unique segments.
		for (int i=0; i<length; i++) {
			long[] segNums = offsets.get(i);
			for (int j=0; j<segNums.length; j++) {
				long segInFile = segNums[j];
				BlockID curSegId = idBySegInFile(segInFile);

				System.out.printf("[I] Searching recs in: %s\n", curSegId);

				DataSegment curSegment = null;
				try {
					curSegment = (DataSegment)cache.acquireBlock(curSegId);
					curSegment.loadBlock(); //The segment data might not be loaded when we get from the cache
					int cnt = curSegment.readAll(minTS, maxTS, recFactory, results);
					//System.out.printf("  seg=%d, cnt=%d\n", segInFile, cnt);
				} finally {
					if (curSegment != null) {
						cache.releaseBlock(curSegment.id()); //We can potentially improve the performance here
					}
				}
			}
		}

		if (results.size() == 0)
			return null;

		return results;
	}

	/**
	 * Performs the read operation. Single writer and multiple readers are allowed to write and read concurrently.
	 * @param buffer
	 * @param length
	 * @param offsetInFile
	 * @return
	 * @throws IllegalArgumentException
	 * @throws InvalidFileOffsetException
	 * @throws IOException
	 * @throws KawkabException
	 */
	public int read(final byte[] buffer, final int length, final long offsetInFile) throws
			IllegalArgumentException, InvalidFileOffsetException, IOException, KawkabException {
		if (length <= 0)
			throw new IllegalArgumentException("Given length is 0.");

		if (offsetInFile + length > fileSize.get())
			throw new IllegalArgumentException(String.format("File offset + read length is greater "
					+ "than file size: %d + %d > %d", offsetInFile,length,fileSize));

		int bufferOffset = 0;
		int remaining = length;
		long curOffsetInFile = offsetInFile;

		while(remaining > 0 && curOffsetInFile < fileSize.get() && bufferOffset<buffer.length) {
			//System.out.println("  Read at offset: " + offsetInFile);
			BlockID curSegId = getByFileOffset(curOffsetInFile);

			//System.out.println("Reading block at offset " + offsetInFile + ": " + curBlkUuid.key);

			long segNumber = FixedLenRecordUtils.segmentInFile(curOffsetInFile, recordSize);
			long nextSegStart = (segNumber + 1) * conf.segmentSizeBytes;
			int toRead = (int)(curOffsetInFile+remaining <= nextSegStart ? remaining : nextSegStart - curOffsetInFile);
			int bytesRead = 0;

			// System.out.println(String.format("Seg=%s, bufLen=%d, bufOffset=%d, toRead=%d, offInFile=%d, remInBuf=%d,dataRem=%d",
			//     curSegId.toString(), buffer.length, bufferOffset, toRead, curOffsetInFile, buffer.length-bufferOffset,remaining));

			assert bufferOffset+toRead <= buffer.length;

			DataSegment curSegment = null;
			try {
				curSegment = (DataSegment)cache.acquireBlock(curSegId);
				curSegment.loadBlock(); //The segment data might not be loaded when we get from the cache
				bytesRead = curSegment.read(buffer, bufferOffset, toRead, curOffsetInFile);
			} finally {
				if (curSegment != null) {
					cache.releaseBlock(curSegment.id());
				}
			}

			bufferOffset += bytesRead;
			remaining -= bytesRead;
			curOffsetInFile += bytesRead;
		}

		return bufferOffset;
	}

	public int appendBuffered(final Record record) throws IOException, InterruptedException, KawkabException {
		assert record.size() == recordSize : String.format("record sizes do not match, given rec size=%d, recSize in inode=%d",record.size(), recordSize);

		int length = record.size();

		if (length != recordSize)
			throw new KawkabException(String.format("The record size (%d bytes) does not match with the file's record size (%d bytes)", length, recordSize));

		long fileSizeBuffered = this.fileSize.get(); // Current file size

		if (fileSizeBuffered + length > MAXFILESIZE) {
			throw new MaxFileSizeExceededException();
		}

		if (acquiredSeg == null || !timerQ.tryDisable(acquiredSeg)) {
			boolean createNew = (FixedLenRecordUtils.offsetInBlock(fileSizeBuffered, recordSize) % conf.dataBlockSizeBytes) == 0L;
			acquiredSeg = acquireSegment(fileSizeBuffered, createNew);
			acquiredSeg.getItem().setIsLoaded();
		}

		DataSegment ds = acquiredSeg.getItem();
		try {
			int bytes = ds.append(record.copyOutSrcBuffer(), fileSizeBuffered);

			if (FixedLenRecordUtils.recordInSegment(fileSizeBuffered, recordSize) == 0) { // If the current record is the first record in the data segment
				long segmentInFile = FixedLenRecordUtils.segmentInFile(fileSizeBuffered, recordSize);
				index.appendMinTS(record.timestamp(), segmentInFile, indexLength(fileSizeBuffered));
			}

			fileSizeBuffered += bytes;
		} catch (IOException e) {
			throw new KawkabException(e);
		}

		timerQ.enableAndAdd(acquiredSeg,clock.currentTime()+ bufferTimeOffsetMs);

		//tlog1.start();
		if (ds.isFull()) {	// If the current segment is full, we don't need to keep the segment as the segment
			acquiredSeg = null;		// is now immutable. The segment will be eventually returned to the cache.

			long segmentInFile = FixedLenRecordUtils.segmentInFile(fileSizeBuffered-recordSize, recordSize);
			index.appendMaxTS(record.timestamp(), segmentInFile, indexLength(fileSizeBuffered)-1);
		}

		fileSize.set(fileSizeBuffered);

		return length;
	}

	//public static TimeLog tlog1 = new TimeLog(TimeLog.TimeLogUnit.NANOS, "ab all");
	public synchronized int appendBuffered(final byte[] data, int offset, final int length) //Syncrhonized with close() due to acquiredSeg
			throws MaxFileSizeExceededException, IOException, InterruptedException, KawkabException {

		assert recordSize == 1;

		int remaining = length; //Reaming size of data to append
		long fileSizeBuffered = this.fileSize.get(); // Current file size

		if (fileSizeBuffered + length > MAXFILESIZE) {
			throw new MaxFileSizeExceededException();
		}


		while (remaining > 0) {
			if (acquiredSeg == null || (!timerQ.tryDisable(acquiredSeg))) { //If null, no need to synchronize because the TimerQueue thread will never synchronize with this timer
				boolean createNew = (fileSizeBuffered % conf.dataBlockSizeBytes) == 0L;
				acquiredSeg = acquireSegment(fileSizeBuffered, createNew);
				acquiredSeg.getItem().setIsLoaded();
			}

			DataSegment ds = acquiredSeg.getItem();

			try {
				int bytes = ds.append(data, offset, remaining, fileSizeBuffered);

				remaining -= bytes;
				offset += bytes;
				fileSizeBuffered += bytes;
			} catch (IOException e) {
				throw new KawkabException(e);
			}

			timerQ.enableAndAdd(acquiredSeg, clock.currentTime()+ bufferTimeOffsetMs);

			//tlog1.start();
			if (ds.isFull()) {	// If the current segment is full, we don't need to keep the segment as the segment
				acquiredSeg = null;		// is now immutable. The segment will be eventually returned to the cache.

			}
		}

		fileSize.set(fileSizeBuffered);

		return length;
	}

	/**
	 * A helper function to acquire segment from the cache, and create a new data block if needed.
	 * @param fileSize Current file size
	 * @param createBlock Whether to create a new file block
	 * @return An wrapper object to release block to the cache through the TimerQueue
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private TimerQueueItem<DataSegment> acquireSegment(long fileSize, boolean createBlock) throws IOException, InterruptedException, KawkabException {
		DataSegmentID segId;
		if (createBlock) { // if the last block is full
			segId = createNewBlock(fileSize);
		} else { // Otherwise the last segment was full or we are just starting without any segment at hand
			segId = getSegmentID(fileSize);
		}
		DataSegment ds = (DataSegment) cache.acquireBlock(segId);
		//ds.initForAppend(fileSize, recordSize);
		return new TimerQueueItem<>(ds, this);
	}

	private DataSegmentID getSegmentID(long offsetInFile) {
		long segmentInFile = FixedLenRecordUtils.segmentInFile(offsetInFile, recordSize);
		long blockInFile = FixedLenRecordUtils.blockInFile(segmentInFile);
		int segmentInBlock = FixedLenRecordUtils.segmentInBlock(segmentInFile);

		return new DataSegmentID(inumber, blockInFile, segmentInBlock, recordSize);
	}

	/**
	 * Creates a new block in the virtual file.
	 * @param fileSize The current fileSize during the last append operation. Note that the
	 * fileSize instance variable is updated as the last step in the append operation.
	 * @return Returns the ID of the first segment in the block.
	 * @throws IOException
	 * @throws InterruptedException
	 */
	DataSegmentID createNewBlock(final long fileSize) throws IOException, InterruptedException {
		DataSegmentID dsid = getSegmentID(fileSize);
		localStore.createBlock(dsid);
		return dsid;
	}

	public long fileSize(){
		return fileSize.get();
	}

	/**
	 * Loads data in the inode variables from the buffer
	 * @param buffer
	 * @return Number of bytes read from the buffer
	 * @throws IOException
	 */
	int loadFrom(final ByteBuffer buffer) {
		long inum = buffer.getLong();
		long fs = buffer.getLong();
		int recSize = buffer.getInt();

		assert fileSize.get() <= fs;

		fileSize.set(fs);
		if (!isLoaded) {
			inumber = inum;
			recordSize = recSize;
			isLoaded = true;
		}

		System.out.printf("[I] Loaded inode %d from buffer: fs=%d, recSize=%d\n", inum, fs, recordSize);

		return Long.BYTES*2 + Integer.BYTES;
	}

	/**
	 * Loads inode variables from the channel
	 * @param channel
	 * @return Number of bytes read from the channel
	 * @throws IOException
	 */
	int loadFrom(final ReadableByteChannel channel) throws IOException {
		ByteBuffer buffer = ByteBuffer.allocate(conf.inodeSizeBytes);
		int bytesRead = Commons.readFrom(channel, buffer);

		if (bytesRead < conf.inodeSizeBytes) {
			throw new InsufficientResourcesException(String.format("Full block is not loaded. Loaded "
					+ "%d bytes out of %d.", bytesRead, conf.inodeSizeBytes));
		}

		buffer.rewind();
		return loadFrom(buffer);
	}

	/*
	 * Serializes the inode in the channel
	 * @return Number of bytes written in the channel
	 */
	int storeTo(final WritableByteChannel channel) throws IOException {
		ByteBuffer buffer = ByteBuffer.allocate(conf.inodeSizeBytes);
		buffer.clear();
		storeTo(buffer);
		buffer.rewind();

		int bytesWritten = Commons.writeTo(channel, buffer);

		if (bytesWritten < conf.inodeSizeBytes) {
			throw new InsufficientResourcesException(String.format("Full inode is not stored. Stored "
					+ "%d bytes out of %d.", bytesWritten, conf.inodeSizeBytes));
		}
		return bytesWritten;

	}

	int storeTo(ByteBuffer buffer) {
		// assert fileSize.get() == fileSizeBuffered.get(); //Not needed because this is not called by the LocalStoreManager
		System.out.printf("[I] Storing inode %d to buffer: fs=%d, recSize=%d\n", inumber, fileSize.get(), recordSize);

		buffer.putLong(inumber);
		buffer.putLong(fileSize.get());
		buffer.putInt(recordSize);

		return Long.BYTES*2 + Integer.BYTES;
	}

	/**
	 * This function must not be concurrent to the append function. So this should be called from the same thread as
	 * the append function. Otherwise, we should synchronize the caller and the appender in append and close functions.
	 */
	synchronized void cleanup() throws KawkabException { //Synchronized with appendBuffered() due to acquiredSeg
		//tlog.printStats("DS.append,ls.store");

		if (acquiredSeg != null && timerQ.tryDisable(acquiredSeg)) {
			deferredWork(acquiredSeg.getItem());
			acquiredSeg = null;
		}

		if (index != null)
			index.shutdown();
	}

	@Override
	public void deferredWork(DataSegment ds) {
		try {
			cache.releaseBlock(ds.id());
		} catch (KawkabException e) {
			e.printStackTrace();
		}
	}

	public long numRecords() {
		return fileSize.get() / recordSize;
	}

	private long indexLength(final long filesize) {
		// ceil(recsInFile / recsPerSeg)
		// 0 1 2 | 3 4 5 | 6 7
		// (fs / recordSize) / (segSize / recordSize)
		long recsInFile = filesize / recordSize;
		long segsInFile = (long) Math.ceil(filesize / (double)conf.segmentSizeBytes); // Taking the ceil value
		return segsInFile * 2 - ((recsInFile % recsPerSeg) == 0 ? 0 : 1);
	}
}
