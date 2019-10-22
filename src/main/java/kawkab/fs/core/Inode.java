package kawkab.fs.core;

import kawkab.fs.api.Record;
import kawkab.fs.commons.Commons;
import kawkab.fs.commons.Configuration;
import kawkab.fs.commons.FixedLenRecordUtils;
import kawkab.fs.core.exceptions.*;
import kawkab.fs.core.index.FileIndex;
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
	private FileIndex index;

	private volatile TimerQueueItem<DataSegment> acquiredSeg;

	private static final Cache cache = Cache.instance();
	private static final Clock clock = Clock.instance();
	private static final LocalStoreManager localStore = LocalStoreManager.instance();
	private static final Configuration conf = Configuration.instance();
	private static final TimerQueue timerQ;

	private static final int bufferTimeOffsetMillis = 5;

	public static final long MAXFILESIZE = conf.maxFileSizeBytes;

	static {
		timerQ = new TimerQueue("DSQueue"); //FIXME: We should pass timerQ's object in the constructor and isntantiate TimerQueue once
	}

	protected Inode(long inumber, int recordSize) {
		this.inumber = inumber;
		this.recordSize = recordSize;

		if (recordSize > 1) {
			index = new PostOrderHeapIndex(inumber, conf.indexNodeSizeBytes, conf.nodesPerBlockPOH, conf.percentIndexEntriesPerNode, cache);
			initIndex();
		}
	}

	private void initIndex() {
		System.out.println("Initializing index for inode: " + inumber);

		try {
			index.loadAndInit();
		} catch (IOException | KawkabException e) {
			e.printStackTrace();
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

		return new DataSegmentID(inumber, blockInFile, segmentInBlock);
	}

	private BlockID idBySegInFile(long segInFile) {
		int segmentInBlock = FixedLenRecordUtils.segmentInBlock(segInFile);
		long blockInFile = FixedLenRecordUtils.blockInFile(segInFile);

		return new DataSegmentID(inumber, blockInFile, segmentInBlock);
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

		long offsetInFile = index.findHighest(key);

		//System.out.println("  Read at offset: " + offsetInFile);
		BlockID curSegId = getByFileOffset(offsetInFile);

		//System.out.println("Reading block at offset " + offsetInFile + ": " + curBlkUuid.key);

		DataSegment curSegment = null;
		try {
			curSegment = (DataSegment)cache.acquireBlock(curSegId);
			curSegment.loadBlock(); //The segment data might not be loaded when we get from the cache
			int bytesRead = curSegment.read(dstRecord.copyInDstBuffer(), offsetInFile, recordSize);

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
		assert recNum >= 0;
		assert dstRecord.size() == recordSize;

		long offsetInFile = (recNum-1L) * recordSize;
		long fileSize = this.fileSize.get();

		if (offsetInFile+recordSize > fileSize)
			throw new KawkabException(String.format("Record number is out of range of the file. OffsetInFile=%d, recordSize=%d, fileSize=%d",
					offsetInFile, recordSize, fileSize));

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
			int bytesRead = curSegment.read(dstRecord.copyInDstBuffer(), offsetInFile, recordSize);

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

		List<long[]> offsets = index.findAll(minTS, maxTS); //Get the offsets

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
				DataSegment curSegment = null;

				try {
					curSegment = (DataSegment)cache.acquireBlock(curSegId);
					curSegment.loadBlock(); //The segment data might not be loaded when we get from the cache
					int cnt = curSegment.readAll(minTS, maxTS, recFactory, results, recordSize);
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

		if (acquiredSeg == null || (!timerQ.tryDisable(acquiredSeg))) {
			acquiredSeg = acquireSegment(fileSizeBuffered, ((FixedLenRecordUtils.offsetInBlock(fileSizeBuffered, recordSize) % conf.dataBlockSizeBytes) == 0L));
		}

		DataSegment ds = acquiredSeg.getItem();
		try {
			int bytes = ds.append(record.copyOutSrcBuffer(), fileSizeBuffered, recordSize);

			if (FixedLenRecordUtils.recordInSegment(fileSizeBuffered, recordSize) == 0) { // If the current record is the first record in the data segment
				long segmentInFile = FixedLenRecordUtils.segmentInFile(fileSizeBuffered, recordSize);
				index.appendMinTS(record.timestamp(), segmentInFile);
			}

			fileSizeBuffered += bytes;
		} catch (IOException e) {
			throw new KawkabException(e);
		}

		timerQ.enableAndAdd(acquiredSeg,clock.currentTime()+bufferTimeOffsetMillis);

		//tlog1.start();
		if (ds.isFull()) {	// If the current segment is full, we don't need to keep the segment as the segment
			acquiredSeg = null;		// is now immutable. The segment will be eventually returned to the cache.

			long segmentInFile = FixedLenRecordUtils.segmentInFile(fileSizeBuffered-recordSize, recordSize);
			index.appendMaxTS(record.timestamp(), segmentInFile);
		}

		fileSize.set(fileSizeBuffered);

		return length;
	}

	//public static TimeLog tlog1 = new TimeLog(TimeLog.TimeLogUnit.NANOS, "ab all");
	public synchronized int appendBuffered(final byte[] data, int offset, final int length) //Syncrhonized with close() due to acquiredSeg
			throws MaxFileSizeExceededException, IOException, InterruptedException, KawkabException {

		assert recordSize > 1;

		int remaining = length; //Reaming size of data to append
		long fileSizeBuffered = this.fileSize.get(); // Current file size

		if (fileSizeBuffered + length > MAXFILESIZE) {
			throw new MaxFileSizeExceededException();
		}


		while (remaining > 0) {
			if (acquiredSeg == null || (!timerQ.tryDisable(acquiredSeg))) { //If null, no need to synchronize because the TimerQueue thread will never synchronize with this timer
				acquiredSeg = acquireSegment(fileSizeBuffered, (fileSizeBuffered % conf.dataBlockSizeBytes) == 0L);
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

			timerQ.enableAndAdd(acquiredSeg, clock.currentTime()+bufferTimeOffsetMillis);

			//tlog1.start();
			if (ds.isFull()) {	// If the current segment is full, we don't need to keep the segment as the segment
				acquiredSeg = null;		// is now immutable. The segment will be eventually returned to the cache.

			}
		}

		fileSize.set(fileSizeBuffered);

		return length;
	}

	/**
	 * A helper function to acquire segment from the cache, and optionally create a new Data Block.
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
		ds.initForAppend(fileSize, recordSize);
		return new TimerQueueItem<>(ds, this);
	}

	private DataSegmentID getSegmentID(long offsetInFile) {
		long segmentInFile = FixedLenRecordUtils.segmentInFile(offsetInFile, recordSize);
		long blockInFile = FixedLenRecordUtils.blockInFile(segmentInFile);
		int segmentInBlock = FixedLenRecordUtils.segmentInBlock(segmentInFile);

		return new DataSegmentID(inumber, blockInFile, segmentInBlock);
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
	int loadFrom(final ByteBuffer buffer) throws IOException {
		if (buffer.remaining() < conf.inodeSizeBytes) {
			throw new InsufficientResourcesException(String.format("Not enough bytes left in the buffer: "
					+ "Have %d, needed %d.",buffer.remaining(), conf.inodeSizeBytes));
		}

		inumber = buffer.getLong();
		fileSize.set(buffer.getLong());
		recordSize = buffer.getInt();

		int hasIndex = buffer.getInt();
		if (hasIndex > 0) {
			index = new PostOrderHeapIndex(inumber, conf.indexNodeSizeBytes, conf.nodesPerBlockPOH, conf.percentIndexEntriesPerNode, cache);
			index.loadFrom(buffer);
			initIndex();
		}

		System.out.printf("Loaded inode %d: fs=%d\n",inumber, fileSize.get());

		return Long.BYTES + Long.BYTES + Integer.BYTES; //FIXME: Define constant size or get value from the position in the buffer
	}

	/**
	 * Loads inode variables from the channel
	 * @param channel
	 * @return Number of bytes read from the channel
	 * @throws IOException
	 */
	int loadFrom(final ReadableByteChannel channel) throws IOException {
		int inodeSizeBytes = conf.inodeSizeBytes;
		ByteBuffer buffer = ByteBuffer.allocate(inodeSizeBytes);

		int bytesRead = Commons.readFrom(channel, buffer);

		if (bytesRead < inodeSizeBytes) {
			throw new InsufficientResourcesException(String.format("Full block is not loaded. Loaded "
					+ "%d bytes out of %d.",bytesRead,inodeSizeBytes));
		}

		buffer.rewind();
		inumber = buffer.getLong();
		fileSize.set(buffer.getLong());
		recordSize = buffer.getInt();

		int hasIndex = buffer.getInt();
		if (hasIndex > 0) {
			index = new PostOrderHeapIndex(inumber, conf.indexNodeSizeBytes, conf.nodesPerBlockPOH, conf.percentIndexEntriesPerNode, cache);
			index.loadFrom(buffer);
			initIndex();
		}

		//System.out.printf("\tLoaded inode: %d -> %d\n", inumber, fileSize.get());

		return bytesRead;
	}

	/*
	 * Serializes the inode in the channel
	 * @return Number of bytes written in the channel
	 */
	int storeTo(final WritableByteChannel channel) throws IOException {
		ByteBuffer buffer = ByteBuffer.allocate(conf.inodeSizeBytes);
		storeTo(buffer);
		buffer.rewind();
		//buffer.limit(buffer.capacity());

		int bytesWritten = Commons.writeTo(channel, buffer);

		if (bytesWritten < conf.inodeSizeBytes) {
			throw new InsufficientResourcesException(String.format("Full block is not strored. Stored "
					+ "%d bytes out of %d.",bytesWritten, conf.inodeSizeBytes));
		}

		return bytesWritten;
	}

	int storeTo(ByteBuffer buffer) {
		// assert fileSize.get() == fileSizeBuffered.get(); //Not needed because this is not called by the LocalStoreManager
		int start = buffer.position();

		buffer.putLong(inumber);
		buffer.putLong(fileSize.get());
		buffer.putInt(recordSize);

		if (index == null) {
			buffer.putInt(0);
		} else {
			buffer.putInt(1);
			index.storeTo(buffer);
		}

		int written = buffer.position() - start;
		return written;
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

	//FIXME: This function should really not be here.
	public static void shutdown() {
		timerQ.shutdown();
	}

	public static void waitUntilSynced() {
		timerQ.waitUntilEmpty();
	}
}
