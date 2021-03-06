package kawkab.fs.core;

import kawkab.fs.api.Record;
import kawkab.fs.commons.Commons;
import kawkab.fs.commons.Configuration;
import kawkab.fs.commons.FixedLenRecordUtils;
import kawkab.fs.core.exceptions.*;
import kawkab.fs.core.index.poh.PostOrderHeapIndex;
import kawkab.fs.core.timerqueue.DeferredWorkReceiver;
import kawkab.fs.core.timerqueue.TimerQueueIface;
import kawkab.fs.core.timerqueue.TimerQueueItem;
import kawkab.fs.utils.LatHistogram;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
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
	private static final ApproximateClock clock = ApproximateClock.instance();
	private static final LocalStoreManager localStore = LocalStoreManager.instance();
	private static final Configuration conf = Configuration.instance();
	private TimerQueueIface timerQ;

	private boolean isInited;
	private int recsPerSeg;

	private static final int bufferTimeOffsetMs = 2;

	public static final long MAXFILESIZE = conf.maxFileSizeBytes;

	protected Inode(long inumber, int recordSize) {
		this.inumber = inumber;
		this.recordSize = recordSize;
	}

	//private LatHistogram idxLog;
	//private LatHistogram segLoadLog;

	/**
	 * Prepare after opening the file.
	 * This function should be called after the inode has been loaded from the file or the remote node or the global store.
	 */
	synchronized void prepare(TimerQueueIface fsQ, TimerQueueIface segsQ) {
		if (isInited)
			return;
		isInited = true;

		//System.out.printf("[I] Initializing index for inode: %d, recSize=%d\n", inumber, recordSize);

		timerQ = segsQ;

		assert recordSize >= 1;
		recsPerSeg = conf.segmentSizeBytes / recordSize;
		if (recordSize > 1)
			index = new PostOrderHeapIndex(inumber, conf.indexNodeSizeBytes, conf.nodesPerBlockPOH, conf.percentIndexEntriesPerNode, cache, fsQ);

		//idxLog = new LatHistogram(TimeUnit.MICROSECONDS, "Index search", 100, 1000000);
		//segLoadLog = new LatHistogram(TimeUnit.MICROSECONDS, "segLoadLog", 10, 1000);

		/*try {
			index.loadAndInit(indexLength(fileSize.get()));
		} catch (IOException | KawkabException e) {
			e.printStackTrace();
		}*/
	}

	synchronized void loadLastBlock() throws KawkabException, IOException {
		// Pre-fetching the last block to warm up for writes
		long fs = fileSize.get();

		// System.out.printf("[I] Should load the last block? %b, fs=%d, recsPerSeg*recSize=%d\n",
		//		fs%(recsPerSeg*recordSize)!=0, fs, recsPerSeg*recordSize);

		if (fs % (recsPerSeg*recordSize) != 0) {
			BlockID curSegId = getByFileOffset(fs);

			System.out.printf("[I] Loading the last seg %s on file open\n", curSegId );

			cache.acquireBlock(curSegId).loadBlock(false);
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
	public boolean readAt(final ByteBuffer dstBuf, final long timestamp, final int recSize, boolean loadFromPrimary) throws IOException, KawkabException {
		if (timestamp < 0) {
			throw new KawkabException(String.format("Invalid timestamp %d", timestamp));
		}

		if (recSize != recordSize) {
			throw new KawkabException(String.format("Record sizes do not match. Given %d, expected %d", recSize, recordSize));
		}

		if (dstBuf.remaining() < recordSize) {
			throw new KawkabException(String.format("Not enough space in the dstBuf, available %d, required %d\n", dstBuf.remaining(), recordSize));
		}

		//idxLog.start();
		long segInFile = index.findHighest(timestamp, indexLength(fileSize.get()), loadFromPrimary);
		//idxLog.end();

		if (segInFile == -1)
			return  false;

		//System.out.println("  Read at offset: " + offsetInFile);
		BlockID curSegId = idBySegInFile(segInFile);

		//System.out.println("Reading block at offset " + offsetInFile + ": " + curBlkUuid.key);

		DataSegment curSegment = null;
		try {
			curSegment = (DataSegment)cache.acquireBlock(curSegId);
			//segLoadLog.start();
			curSegment.loadBlock(loadFromPrimary); //The segment data might not be loaded when we get from the cache
			//segLoadLog.end();
			boolean recFound = curSegment.readRecord(dstBuf, timestamp);
			dstBuf.flip();

			if (!recFound)
				return false;
		} finally {
			if (curSegment != null) {
				cache.releaseBlock(curSegment.id());
			}
		}

		return true;
	}

	/**
	 * Reads the record at index recNum.
	 *
	 * @return
	 * @throws IllegalArgumentException
	 * @throws InvalidFileOffsetException
	 * @throws IOException
	 * @throws KawkabException
	 */
	public boolean readRecordN(final ByteBuffer dstBuf, final long recNum, final int recSize, boolean loadFromPrimary) throws IOException, KawkabException {
		if (recNum <= 0) {
			throw new KawkabException(String.format("Invalid record number %d", recNum));
		}

		if (recSize != recordSize) {
			throw new KawkabException(String.format("Record sizes do not match. Given %d, expected %d", recSize, recordSize));
		}

		if (dstBuf.remaining() < recordSize) {
			throw new KawkabException(String.format("Not enough space in the dstBuf, available %d, required %d\n", dstBuf.remaining(), recordSize));
		}

		long offsetInFile = (recNum-1) * recordSize;
		long fileSize = this.fileSize.get();

		assert offsetInFile >= 0 : String.format("Invalid offsetInFile %d, recNum %d", offsetInFile, recNum);

		if (offsetInFile+recordSize > fileSize)
			throw new KawkabException(String.format("Record number %d is out of range of the file. OffsetInFile=%d, recordSize=%d, recsInFile=%d",
					recNum, offsetInFile, recordSize, fileSize/recordSize));

		//System.out.println("  Read at offset: " + offsetInFile);
		BlockID curSegId = getByFileOffset(offsetInFile);

		//System.out.println("Reading block at offset " + offsetInFile + ": " + curBlkUuid.key);

		DataSegment curSegment = null;
		try {
			curSegment = (DataSegment)cache.acquireBlock(curSegId);

			//segLoadLog.start();
			curSegment.loadBlock(loadFromPrimary); //The segment data might not be loaded when we get from the cache
			//segLoadLog.end();

			int bytesRead = curSegment.read(dstBuf, offsetInFile);
			dstBuf.flip();

			assert bytesRead == recordSize;
		} finally {
			if (curSegment != null) {
				cache.releaseBlock(curSegment.id());
			}
		}

		return true;
	}

	private static ThreadLocal<ByteBuffer> thrLocalBuf = ThreadLocal.withInitial(() -> ByteBuffer.allocate(conf.maxBufferLen));

	public List<ByteBuffer> readRecords(final long minTS, final long maxTS, final int recSize, boolean loadFromPrimary) throws KawkabException, IOException {
		if (minTS < 0 || maxTS < 0) {
			throw new KawkabException(String.format("Invalid minTS (%d) or maxTS (%d) is given", minTS, maxTS));
		}

		if (recSize != recordSize) {
			throw new KawkabException(String.format("Record sizes do not match. Given %d, expected %d", recSize, recordSize));
		}

		//idxLog.start();
		List<long[]> offsets = index.findAll(minTS, maxTS, indexLength(fileSize.get()), loadFromPrimary); //Get the offsets
		//idxLog.end();

		if (offsets == null)
			return null;

		ByteBuffer dstBuf = thrLocalBuf.get();
		dstBuf.clear();

		int length = offsets.size();
		List<ByteBuffer> results = new ArrayList<>(); //The lists contains offsets to unique segments.
		for (int i=0; i<length; i++) {
			long[] segNums = offsets.get(i);
			for (int j=0; j<segNums.length; j++) {
				long segInFile = segNums[j];
				BlockID curSegId = idBySegInFile(segInFile);

				//System.out.printf("[I] Searching recs in: %s\n", curSegId);

				DataSegment curSegment = null;
				try {
					curSegment = (DataSegment)cache.acquireBlock(curSegId);

					//segLoadLog.start();
					curSegment.loadBlock(loadFromPrimary); //The segment data might not be loaded when we get from the cache
					//segLoadLog.end();

					//ByteBuffer dstBuf = ByteBuffer.allocate(conf.segmentSizeBytes);
					int cnt = curSegment.readRecords(minTS, maxTS, dstBuf);
					//dstBuf.flip();

					//if (cnt > 0)
					//	results.add(dstBuf);
					//System.out.printf("  seg=%d, cnt=%d\n", segInFile, cnt);
				} finally {
					if (curSegment != null) {
						cache.releaseBlock(curSegment.id()); //We can potentially improve the performance here
					}
				}
			}
		}

		if (dstBuf.position() > 0) {
			results.add(dstBuf);
			dstBuf.flip();
		}

		if (results.size() == 0)
			return null;

		return results;

	}

	public List<Record> readAll(long minTS, long maxTS, final Record recFactory, boolean loadFromPrimary) throws IOException, KawkabException {
		if (recFactory.size() != recordSize) {
			throw new KawkabException(String.format("Record sizes do not match. Given %d, expected %d", recFactory.size(), recordSize));
		}

		//idxLog.start();
		List<long[]> offsets = index.findAll(minTS, maxTS, indexLength(fileSize.get()), loadFromPrimary); //Get the offsets
		//idxLog.end();

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

				//System.out.printf("[I] Searching recs in: %s\n", curSegId);

				DataSegment curSegment = null;
				try {
					curSegment = (DataSegment)cache.acquireBlock(curSegId);
					curSegment.loadBlock(loadFromPrimary); //The segment data might not be loaded when we get from the cache
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
	public int read(final byte[] buffer, final int length, final long offsetInFile, boolean loadFromPrimary) throws
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
				curSegment.loadBlock(loadFromPrimary); //The segment data might not be loaded when we get from the cache
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

	int appendRecords(final ByteBuffer srcBuf, int recSize) throws OutOfMemoryException, IOException, InterruptedException, KawkabException {
		if (recSize != recordSize)
			throw new KawkabException(String.format("The given record size (%d bytes) does not match with the file's record size (%d bytes)", recSize, recordSize));

		long fileSizeBuffered = this.fileSize.get(); // Current file size

		int appended = 0;
		int pos = srcBuf.position();
		int initLimit = srcBuf.limit();

		if (fileSizeBuffered + (initLimit-pos)*recSize > MAXFILESIZE) {
			throw new MaxFileSizeExceededException();
		}

		while(pos < initLimit) {
			long recInFile = fileSizeBuffered / recSize;
			int recInSeg = (int) (recInFile % recsPerSeg);
			int toAppend = (recsPerSeg - recInSeg) * recSize;
			int limit = Math.min(pos + toAppend, initLimit);
			srcBuf.limit(limit);

			int wBytes = appendBuffered(srcBuf, recSize);

			pos += wBytes;
			appended += wBytes;
			fileSizeBuffered += wBytes;
		}

		return appended;
	}

	private int appendBuffered(final ByteBuffer srcBuf, int recSize) throws OutOfMemoryException, IOException, InterruptedException, KawkabException {
		if (recSize != recordSize)
			throw new KawkabException(String.format("The given record size (%d bytes) does not match with the file's record size (%d bytes)", recSize, recordSize));

		long fileSizeBuffered = this.fileSize.get(); // Current file size

		/*if (fileSizeBuffered + recSize > MAXFILESIZE) {
			throw new MaxFileSizeExceededException();
		}*/

		if (acquiredSeg == null || !timerQ.tryDisable(acquiredSeg)) {
			boolean createNew = (FixedLenRecordUtils.offsetInBlock(fileSizeBuffered, recordSize) % conf.dataBlockSizeBytes) == 0L;
			try{
				acquiredSeg = acquireSegment(fileSizeBuffered, createNew);
			} catch (Exception e) {
				e.printStackTrace();
				throw e;
			}
			//acquiredSeg.getItem().setIsLoaded();
			acquiredSeg.getItem().prepareForAppend(fileSizeBuffered);
		}

		long timestamp = srcBuf.getLong(srcBuf.position());
		DataSegment ds = acquiredSeg.getItem();

		int appended = ds.append(srcBuf, fileSizeBuffered);

		long recInFile = fileSizeBuffered/recordSize;
		boolean isFirstRec = recInFile % recsPerSeg == 0;
		long segInFile = recInFile/recsPerSeg;
		if (isFirstRec) { // If the current record is the first record in the data segment
			index.appendMinTS(timestamp, segInFile, indexLength(fileSizeBuffered));
		}

		fileSizeBuffered += appended;

		timerQ.enableAndAdd(acquiredSeg,clock.currentTime()+ bufferTimeOffsetMs);

		//tlog1.start();
		if (ds.isFull()) {	// If the current segment is full, we don't need to keep the segment as the segment
			acquiredSeg = null;		// is now immutable. The segment will be eventually returned to the cache.

			//long segmentInFile = FixedLenRecordUtils.segmentInFile(fileSizeBuffered-recordSize, recordSize);

			//System.out.printf("[I] DS is full: segInFile=%d, indexLen=%d\n", segInFile, indexLength(fileSizeBuffered-recordSize));

			long lastTS = srcBuf.getLong(srcBuf.position()-recSize);
			index.appendMaxTS(lastTS, segInFile, indexLength(fileSizeBuffered-recSize));
		}

		fileSize.set(fileSizeBuffered);

		return appended;
	}

	//public static LatHistogram tlog1 = new LatHistogram(LatHistogram.TimeLogUnit.NANOS, "ab all");
	public synchronized int appendBuffered(final byte[] data, int offset, final int length) //Syncrhonized with close() due to acquiredSeg
			throws OutOfMemoryException, MaxFileSizeExceededException, IOException, InterruptedException, KawkabException {

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
				//acquiredSeg.getItem().setIsLoaded();
				acquiredSeg.getItem().prepareForAppend(fileSizeBuffered);
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
		assert ds.id() != null;
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
		if (!isInited) {
			inumber = inum;
			recordSize = recSize;
		}

		//System.out.printf("[I] Loaded inode %d from buffer: fs=%d, recSize=%d, recordSize=%d\n", inum, fs, recSize, recordSize);

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
		//System.out.printf("[I] Storing inode %d to buffer: fs=%d, recSize=%d\n", inumber, fileSize.get(), recordSize);

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

		releaseAcquiredSeg();

		if (index != null)
			index.shutdown();

		//idxLog.printStats();
	}

	synchronized void flush() throws KawkabException {
		releaseAcquiredSeg();
		if (index != null)
			index.flush();
	}

	private void releaseAcquiredSeg() {
		if (acquiredSeg != null && timerQ.tryDisable(acquiredSeg)) {
			deferredWork(acquiredSeg.getItem());
			acquiredSeg = null;
		}
	}

	public void printStats() {
		//idxLog.printStats();
		if (index != null)
			index.printStats();
		//segLoadLog.printStats();
		long fs = fileSize();
		System.out.printf("\tFile=%d, FileSize=%d, recsInFile=%d, segsInFile=%d, recsPerSeg=%d, indexLen=%d, numIdxNodes=%d\n",inumber, fs, recordsInFile(),
				FixedLenRecordUtils.segmentInFile(fs, recordSize), recsPerSeg, indexLength(fs), index!=null?index.size(indexLength(fs)):0);
	}

	public void resetStats() {
		//idxLog.reset();
		//segLoadLog.reset();
		if (index != null)
			index.resetStats();
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
		//long segsInFile = (long) Math.ceil(recsInFile / (double)recsPerSeg); // Taking the ceil value
		long segsInFile = (recsInFile+recsPerSeg-1)/recsPerSeg; //FIXME: The long value may overflow
		return segsInFile * 2 - ((recsInFile % recsPerSeg) == 0 ? 0 : 1);
	}

	public int recordSize() {
		return recordSize;
	}

	public long recordsInFile() {
		return fileSize.get()/recordSize;
	}
}
