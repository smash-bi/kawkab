package kawkab.fs.core;

import kawkab.fs.api.Record;
import kawkab.fs.commons.Commons;
import kawkab.fs.commons.Configuration;
import kawkab.fs.commons.FixedLenRecordUtils;
import kawkab.fs.core.exceptions.FileNotExistException;
import kawkab.fs.core.exceptions.InvalidFileOffsetException;
import kawkab.fs.core.exceptions.OutOfMemoryException;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static kawkab.fs.commons.FixedLenRecordUtils.*;

public final class DataSegment extends Block {
	private final static Configuration conf = Configuration.instance();
	public final static int SEGMENT_SIZE_BYTES = conf.segmentSizeBytes;
	
	private ByteBuffer dataBuf; // Buffer to hold actual segment data
	private boolean isLastSeg;
	private volatile boolean isSegFull;  // Sets to true only when the block becomes full in an append operation.
	private long lastFetchTimeMs = 0; // ApproximateClock time in ms when the block was last loaded. This must be initialized
	// to zero when the block is first created in memory.
	
	private AtomicInteger writePos; // Keeps track of the next byte offset where data is appended
	
	private int dirtyOffset;	// The offset where the dirty bytes start, the bytes that are not persisted yet.
	// dirtyOffset doesn't need to be thread-safe because only the localStore thread reads and updates its value.
	// The value is initially set when the block is loaded, at which point the block cannot be accessed by the localStore.
	
	//private boolean initedForAppends = false; 	// Indicates if the segment is initialized for append operations.
	// This variable is not atomic because only the writer modify the variable
	// and the value is assigned atomically due to Java memory model.
	//private int initialAppendPos; // Index from which data will be appended the first time. This position is not modified with the appends
	//private int bytesLoaded; // Number of bytes loaded in this block from the local or remote storage
	
	//private RandomAccessFile rwFile;
	private ByteBuffer storeBuffer;

	private int recordSize;
	private int initialAppendPos;
	private boolean initedForAppends;
	
	/**
	 * The constructor should not create a new file in the local storage. This constructor
	 * does not reads data from the underlying file. Instead, use loadFrom
	 * function for that purpose.
	 */
	DataSegment(DataSegmentID segmentID) {
		super(segmentID);
		writePos = new AtomicInteger(0);

		dataBuf = ByteBuffer.allocateDirect(SEGMENT_SIZE_BYTES);
		storeBuffer = dataBuf.duplicate();
		initedForAppends = false;
		if (id == null)
			return;

		recordSize = segmentID.recordSize();
		isLastSeg = segmentID.segmentInBlock()+1 == conf.segmentsPerBlock;
		initialAppendPos = 0;
	}

	synchronized void reInit(DataSegmentID segmentID) {
		assert segmentID != null;

		super.reset(segmentID);
		isLastSeg = segmentID.segmentInBlock()+1 == conf.segmentsPerBlock;
		
		dataBuf.clear();
		storeBuffer.clear();
		
		lastFetchTimeMs = 0;
		dirtyOffset = 0;
		writePos.set(0);
		isSegFull = false;
		recordSize = segmentID.recordSize();
		initedForAppends = false;
		initialAppendPos = 0;
	}

	synchronized void prepareForAppend(long offsetInFile) {
		if (initedForAppends)
			return;

		initialAppendPos = offsetInSegment(offsetInFile, recordSize);
		writePos.set(initialAppendPos);
		dataBuf.position(initialAppendPos);
		dirtyOffset = initialAppendPos;

		initedForAppends = true;
		if (initialAppendPos == 0)
			setIsLoaded();
	}

	boolean isFull() {
		return isSegFull;
	}
	
	/**
	 * @param data Data to be appended
	 * @param offset  Offset in data
	 * @param length  Number of bytes to append: data[offset] to data[offset+length-1] inclusive.
	 * @param offsetInFile
	 * @return number of bytes appended starting from the offset
	 * @throws IOException
	 */
	synchronized int append(byte[] data, int offset, int length, long offsetInFile) throws IOException {
		assert offset >= 0;
		assert offset < data.length;
		
		//int offsetInSegment = offsetInSegment(offsetInFile, recordSize);
		int offsetInSegment = (int)(offsetInFile% SEGMENT_SIZE_BYTES);
		int capacity = SEGMENT_SIZE_BYTES - offsetInSegment;
		int toAppend = length <= capacity ? length : capacity;
		
		//if (offsetInSegment != writePntr)
		//	System.out.println(id + " - " + offsetInSegment+" != "+writePntr);
		
		assert writePos.get() == offsetInSegment;
		assert dataBuf.position() == offsetInSegment;
		
		dataBuf.put(data, offset, toAppend);
		//dataBuf.position(writePntr+toAppend);
		
		int pos = writePos.addAndGet(toAppend);

		assert pos <= dataBuf.capacity();
		
		//Mark block as full
		if (pos == SEGMENT_SIZE_BYTES) {
			isSegFull = true;
		}
		
		markLocalDirty();
		
		return toAppend;
	}

	synchronized void rollback(int numBytes) {
		System.out.println("rollback");
		writePos.addAndGet(-numBytes);
		dataBuf.position(dataBuf.position()-numBytes);
	}

	/**
	 * @param srcBuffer Source buffer
	 * @param offsetInFile
	 * @return number of bytes appended starting from the offset
	 * @throws IOException
	 */
	synchronized int append(final ByteBuffer srcBuffer, long offsetInFile) throws IOException, OutOfMemoryException {
		int offsetInSegment = offsetInSegment(offsetInFile, recordSize);
		
		//assert writePos.get() == offsetInSegment :
		//		String.format("writePos (%d) != OffsetInSeg (%d) for seg %s, fsLen=%d", writePos.get(), offsetInSegment, id(), offsetInFile);
		//assert dataBuf.position() == offsetInSegment :
		//		String.format("%s: dataBuf pos %d is incorrect, expected %d", id, dataBuf.position(), offsetInSegment);

		if (writePos.get() != offsetInSegment) {
			throw new OutOfMemoryException(String.format("writePos (%d) != OffsetInSeg (%d) for seg %s, fsLen=%d", writePos.get(), offsetInSegment, id(), offsetInFile));
		}

		if (dataBuf.position() != offsetInSegment) {
			throw new OutOfMemoryException(String.format("%s: dataBuf pos %d is incorrect, expected %d", id, dataBuf.position(), offsetInSegment));
		}
		
		int length = srcBuffer.remaining();

		assert SEGMENT_SIZE_BYTES - writePos.get() >= length;
		assert length % recordSize == 0;
		
		dataBuf.put(srcBuffer);

		int pos = writePos.addAndGet(length);

		//Mark block as full
		if (pos+recordSize > SEGMENT_SIZE_BYTES) { //Cannot add any more records
			isSegFull = true;
		}

		markLocalDirty();

		return length;
	}
	
	/**
	 * @param dstBuffer output buffer
	 * @param dstBufferOffset offset in the buffer to where data will be copied
	 * @param length length of data to be read
	 * @return number of bytes read starting from the offsetInFile
	 * @throws IOException
	 */
	synchronized int read(byte[] dstBuffer, int dstBufferOffset, int length, long offsetInFile)
			throws InvalidFileOffsetException {
		int blockSize = SEGMENT_SIZE_BYTES;
		int offsetInSegment = (int)(offsetInFile% SEGMENT_SIZE_BYTES);
		
		if (offsetInSegment >= blockSize || offsetInSegment < 0) {
			throw new InvalidFileOffsetException(
					String.format("Given file offset %d is outside of the block. Block size = %d bytes.",
							offsetInSegment, blockSize));
		}
		
		int readSize = offsetInSegment+length <= blockSize ? length : blockSize-offsetInSegment;
		
		ByteBuffer buf = dataBuf.duplicate(); //FIXME: Ensure that this is thread-safe in the presence of a concurrent writer!!!
		
		buf.rewind(); // We are not worried about the limit or writePos because the reader cannot read beyond the file size
		buf.position(offsetInSegment);
		buf.get(dstBuffer, dstBufferOffset, readSize);
		
		return readSize;
	}
	
	/**
	 * @param dstBuffer output buffer
	 * @return number of bytes read starting from the offsetInFile
	 * @throws IOException
	 */
	synchronized int read(final ByteBuffer dstBuffer, long offsetInFile) {
		assert dstBuffer.remaining() >= recordSize;

		int offsetInSegment = offsetInSegment(offsetInFile, recordSize);

		ByteBuffer buf = dataBuf.duplicate(); //FIXME: Ensure that this is thread-safe in the presence of a concurrent writer!!!
		
		buf.position(offsetInSegment);
		buf.limit(offsetInSegment+recordSize);
		dstBuffer.put(buf);

		System.out.printf("[DS] offsetInFile=%d, offsetInSeg=%d, bufLimit=%d, copyBufTS=%d, dataBufTS=%d\n",
				offsetInFile, offsetInSegment, offsetInSegment+recordSize, buf.getLong(offsetInSegment), dataBuf.getLong(offsetInSegment));

		return buf.position() - offsetInSegment;
	}

	synchronized boolean readRecord(final ByteBuffer dstBuf, final long timestamp) {
		ByteBuffer buf = dataBuf.duplicate();
		buf.rewind(); //To read the first record

		// FIXME: The timestamps are read based on the assumption that the first 8 bytes of a record is a timestamp.
		// We need a systematic way to parse and read a record.


		int limit = writePos.get();
		buf.limit(limit);

		assert buf.remaining() >= recordSize; // at least have one record

		if (timestamp < buf.getLong(0)) { // if the records in this segment are all greater than the given ts
			System.out.printf("[DS] Record with timestamp %d not found, the timestamp is smaller than the first record\n", timestamp);
			return false;
		}

		if (buf.getLong(limit-recordSize) < timestamp) { // All the records in this segment are smaller than the given ts
			System.out.printf("[DS] Record with timestamp %d not found, the timestamp is larger than the last record\n", timestamp);
			return false;
		}

		int recIndex = binarySearch(buf, timestamp); //Record number in this segment that has the matching timestamp
		int pos = recIndex*recordSize;
		buf.position(pos);
		buf.limit(pos+recordSize);
		dstBuf.put(buf);

		return true;
	}

	/**
	 * Add all the records in the results list that have the timestamp within the given minTS and maxTS inclusively
	 *
	 * @param minTS
	 * @param maxTS
	 * @param dstBuf
	 * @return the number of records added in the list
	 */
	synchronized int readRecords(long minTS, long maxTS, ByteBuffer dstBuf) {
		ByteBuffer buf = dataBuf.duplicate();
		buf.rewind(); //To read the first record

		// FIXME: The timestamps are read based on the assumption that the first 8 bytes of a record is a timestamp.
		// We need a systematic way to parse and read a record.

		int limit = writePos.get();
		buf.limit(limit);

		assert buf.remaining() >= recordSize :
				String.format("[DS] No records in segment: ds=%s, remaining=%d, limit=%d, minTS=%d, maxTS=%d",
						id, buf.remaining(), limit, minTS, maxTS); // at least have one record

		if (maxTS < buf.getLong(0)) { // if the records in this segment are all greater than the given range
			System.out.printf("[DS] %s: maxTS (%d) is smaller than the first record minTS(%d). Buf rem=%d, pos=%d, lim=%d, writepos=%d\n",
					id, maxTS, buf.getLong(0), dstBuf.remaining(), dstBuf.position(), dstBuf.limit(), writePos.get());
			return 0;
		}

		if (buf.getLong(limit-recordSize) < minTS) { // All the records in this segment are smaller than the given range
			System.out.printf("[DS] %s: minTS (%d) is larger than the last record maxTS(%d)\n",
					id, minTS, buf.getLong(limit-recordSize));
			return 0;
		}

		int recIndex = binarySearch(buf, maxTS); //Record number in this segment that has the matching timestamp
		int pos = recIndex*recordSize;
		int cnt = 0;
		while (recIndex >= 0 && buf.getLong(pos) >= minTS) {
			cnt++;

			buf.position(pos);
			buf.limit(pos+recordSize);

			dstBuf.put(buf);

			recIndex--;
			pos = recIndex*recordSize;
		}

		return cnt;
	}

	/**
	 * Add all the records in the results list that have the timestamp within the given minTS and maxTS inclusively
	 *
	 * @param minTS
	 * @param maxTS
	 * @param recordFactory Factory object
	 * @param results
	 * @return the number of records added in the list
	 */
	synchronized int readAll(long minTS, long maxTS, Record recordFactory, List<Record> results) {
		ByteBuffer buf = dataBuf.duplicate();
		buf.rewind(); //To read the first record

		// FIXME: The offset of timestamps in the records should be based on the recordFactory. Currently it is based on the
		// assumption that the first 8 bytes of the record is the timestamp

		int limit = writePos.get();
		buf.limit(limit);

		assert buf.remaining() >= recordSize :
				String.format("buf.remaining < recordSize, buf.rem=%d, recSize=%d", buf.remaining(), recordSize); // at least have one record

		if (maxTS < buf.getLong(0)) { // if the records in this segment are all greater than the given range
			return 0;
		}

		if (buf.getLong(limit-recordSize) < minTS) { // All the records in this segment are smaller than the given range
			return 0;
		}

		int recIndex = binarySearch(buf, maxTS); //Record number in this segment that has the matching timestamp
		int pos = recIndex*recordSize;
		int cnt = 0;
		while (recIndex >= 0 && buf.getLong(pos) >= minTS) {
			cnt++;

			buf.position(pos);
			buf.limit(pos+recordSize);

			Record rec = recordFactory.newRecord();
			rec.copyInDstBuffer().put(buf);
			results.add(rec);

			recIndex--;
			pos = recIndex*recordSize;

			//System.out.printf("[DS] Rec found in %s; %s\n",id, rec);
		}

		return cnt;
	}

	synchronized private int binarySearch(ByteBuffer buf, long maxTS) {
		int limit = buf.limit();
		int lowRecNum=0;
		int numRecords = limit/recordSize;
		int highRecNum = numRecords-1; // Doubling the total records because we want to start search from the last record

		// System.out.printf("numRecs=%d, recSize=%d, lim=%d, highRecNum=%d\n", numRecords, recordSize, buf.limit(), highRecNum);

		//Find the first entry from the right that falls within the range
		int midRecNum = highRecNum; //We want to start with the right extreme
		while (lowRecNum < highRecNum) {
			long bufTS = buf.getLong(midRecNum*recordSize);
			// System.out.printf("l=%d, m=%d, r=%d, buf.pos=%d, bufTS=%d\n", lowRecNum, midRecNum, highRecNum, buf.position(), bufTS);

			if (bufTS <= maxTS) { // Target is in the right half
				lowRecNum = midRecNum;
			} else {
				highRecNum = midRecNum - 1;
			}

			midRecNum = (lowRecNum + highRecNum + 1) >>> 1; //Taking the ceiling value to find the right most value
		}

		//System.out.printf("  => Index=%d, ts=%d, minTS=%d, maxTS=%d\n", lowRecNum, buf.getLong(lowRecNum*recordSize), minTS, maxTS);

		if (maxTS < buf.getLong(lowRecNum*recordSize))
			return 0;

		return lowRecNum; //Record number in this segment that has the matching timestamp
	}
	
	@Override
	public boolean shouldStoreGlobally() {
		// Store in the global store only if this is the last segment of the block and this segment is full
		if (isLastSeg // If it is the last segment in the block
				&& isSegFull) { // and the segment is full
			return true;
		}
		
		return false;
	}
	
	@Override
	public boolean evictLocallyOnMemoryEviction() {
		return shouldStoreGlobally();
	}
	
	@Override
	public synchronized int loadFromFile() throws IOException {
		//System.out.println("[DS] Loading form the local file: " + id);
		/*if (initedForAppends && initialAppendPos == 0) { // The DS is already loaded by the appender
			return 0;
		}*/
		
		try (
				RandomAccessFile file = new RandomAccessFile(id.localPath(), "r");
				SeekableByteChannel channel = file.getChannel()
		) {
			
			channel.position(((DataSegmentID)id).segmentInBlock() * SEGMENT_SIZE_BYTES);
			//assert bytesLoaded == 0;
			return loadFrom(channel);
		}
	}

	public synchronized int loadFromWithSkips(ReadableByteChannel channel) throws IOException {
		ByteBuffer buffer = dataBuf.duplicate();
		buffer.clear();

		buffer.limit(bytesPerSegment(recordSize));

		int bytesRead = Commons.readFrom(channel, buffer);
		dirtyOffset = bytesRead;
		writePos.set(bytesRead);
		isSegFull = bytesRead >= bytesPerSegment(recordSize);
		initialAppendPos = bytesRead;
		dataBuf.position(initialAppendPos);
		initedForAppends = true;

		//printTimestamps();

		return bytesRead;
	}

	/*private final static Object printLock = new Object();
	private void printTimestamps() {
		synchronized (printLock) {
			ByteBuffer buf = dataBuf.duplicate();
			buf.clear();
			int max = recordsPerSegment(recordSize);
			System.out.printf("[DS] %s: Records: [", id);
			for (int i = 0; i < max; i++) {
				System.out.printf("%d:%d, ", i, buf.getLong(i * recordSize));
			}
			System.out.println("]");
		}
	}*/

	@Override
	public synchronized int loadFrom(ReadableByteChannel channel) throws IOException {
		//System.out.printf("[DS] Loading %s from channel: databuf rem=%d, pos=%d, writePos=%d, initApndPos=%d\n",
		//		id, dataBuf.remaining(), dataBuf.position(), writePos.get(), initialAppendPos);

		//This is to enable loading data for reads after this buffer is appended for writes.
		int length = SEGMENT_SIZE_BYTES;
		if (initedForAppends) {
			length = initialAppendPos;
		}

		if (length == 0)
			return 0;

		ByteBuffer buffer = dataBuf.duplicate();
		buffer.clear();

		if (initedForAppends) {
			buffer.limit(initialAppendPos);
			//System.out.printf("[DS] %s already inited. buffer remaining=%d, limit=%d, pos=%d, writepos=%d\n",
			//		id, buffer.remaining(), buffer.limit(), buffer.position(), writePos.get());
		}

		int bytesRead = Commons.readFrom(channel, buffer);

		if (bytesRead > 0 && !initedForAppends) {
			dirtyOffset = bytesRead;
			writePos.set(bytesRead);
			isSegFull = bytesRead >= bytesPerSegment(recordSize);
			initialAppendPos = bytesRead;
			dataBuf.position(initialAppendPos);
			initedForAppends = true; //FIXME: This will result in problems if a segment is loaded before init for appends, and then again loaded partially.
			//System.out.printf("[DS] %s not inited for appends. Setting values\n", id);
		} /*else {
			int pos = writePos.addAndGet(bytesRead);
			dirtyOffset += bytesRead;
			isSegFull = pos + recordSize > segmentSizeBytes;

			assert pos <= dataBuf.capacity();
		}*/


		//System.out.printf("[DS] After loading %s from channel: bytesRead=%d, writePos=%d, datBufPos=%d\n",
		//		id, bytesRead, writePos.get(), dataBuf.position());

		return bytesRead;
	}
	
	/**
	 * srcBuffer should have only valid bytes. The caller should rewind the srcBuffer before passing to this function
	 *
	 * @param srcBuffer Buffer from where data is read
	 * @return The number of bytes read from the buffer
	 * @throws IOException
	 */
	@Override
	public synchronized int loadFrom(ByteBuffer srcBuffer) throws IOException {
		/*if (srcBuffer.remaining() < Constants.segmentSizeBytes) { // If we use a ByteBuffer to store data bytes, we send only valid bytes
																// to a remote node. Therefore, this check is not required if we use
																// ByteBuffer. Otherwise, if we use a byte array, we need to check this as
																// we send all the bytes regardless of their validity.
			throw new InsufficientResourcesException(String.format("Not enough bytes left in the buffer: "
					+ "Have %d, needed %d.",srcBuffer.remaining(), Constants.segmentSizeBytes));
		}*/
		
		int pos = dataBuf.position();
		//storeBuffer.clear();
		//storeBuffer.position(pos);

		//System.out.printf("[DS] Before Loading %s from buffer: srcRem=%d, srcPos=%d, dstRem=%d, dstPos=%d, dirtyOffset=%d\n",
		//		id, srcBuffer.remaining(), srcBuffer.position(), dataBuf.remaining(), pos, dirtyOffset);

		dataBuf.put(srcBuffer);

		int bytesRead = dataBuf.position() - pos;
		pos = writePos.addAndGet(bytesRead);
		dirtyOffset += bytesRead;
		isSegFull = pos+recordSize > SEGMENT_SIZE_BYTES;

		//System.out.printf("[DS] After loading %s from buffer: bytesRead=%d, writePos=%d, datBufPos=%d, dirtyOffset=%d\n",
		//		id, bytesRead, writePos.get(), dataBuf.position(), dirtyOffset);

		return bytesRead;
	}

	/**
	 * srcBuffer remaining capacity must not be larger than segmentSizeBytes. Moreover, srcBuffer should have segment
	 * bytes from beginning.
	 *
	 * @param srcBuffer
	 * @return
	 * @throws IOException
	 */
	public synchronized int loadFromWithSkips(ByteBuffer srcBuffer) throws IOException {
		int pos = dataBuf.position();

		int srcLimitInit = srcBuffer.limit();
		int srcPos = srcBuffer.position();
		if (pos > 0) {
			assert srcPos + pos <= srcLimitInit;
			srcBuffer.position(srcPos+pos); //Skipping some bytes from reading
		}

		return loadFrom(srcBuffer);
	}

	public synchronized int storeTo(ByteBuffer dstBuffer, int offset) {
		if (offset == writePos.get())
			return 0;

		int limit = isSegFull ? SEGMENT_SIZE_BYTES : writePos.get();

		storeBuffer.clear();
		storeBuffer.position(offset);
		storeBuffer.limit(limit);

		int stored = storeBuffer.remaining();
		dstBuffer.put(storeBuffer);

		//System.out.printf("[DS] ByteBuffer store %s: pos=%d, limit=%d, rem=%d, offset=%d, rec0TSSRC=%d, rec0TSDST=%d\n",
		//		id, storeBuffer.position(), limit, storeBuffer.remaining(), offset,
		//		dstBuffer.getLong(dstBuffer.position() - (limit-offset)), storeBuffer.getLong(offset));


		return stored;
	}

	//public static LatHistogram dbgHist = new LatHistogram(TimeUnit.MICROSECONDS, "DS load", 100, 100000);

	private synchronized void loadBlockFromPrimary() throws FileNotExistException, IOException {
		//System.out.printf("[DS] Fetch %s from primary. writePos=%d, dirtyOffset=%d, dataBufPost=%d\n", id, writePos.get(), dirtyOffset, dataBuf.position());
		//dbgHist.start();
		loadFrom(primaryNodeService.getSegment((DataSegmentID)id, writePos.get()));
		//dbgHist.end();
	}
	
	@Override
	public synchronized int storeToFile() throws IOException {
		FileChannel channel = FileChannel.open(new File(id().localPath()).toPath(), StandardOpenOption.WRITE);
		//System.out.println("Store: "+id() + ": " + channel.position());
		int count = storeTo(channel);

		channel.force(true);
		channel.close();


		return count;
	}

	/**
	 * Stores only dirty bytes at the end of the channel.
	 * @param channel
	 * @return
	 * @throws IOException
	 */
	@Override
	public synchronized int storeTo(FileChannel channel) throws IOException {
		int bytesWritten = 0;

		//channel.position(appendOffsetInBlock());
		channel.position(channel.size());

		int limit = isSegFull ? SEGMENT_SIZE_BYTES : writePos.get();

		storeBuffer.clear();
		storeBuffer.position(dirtyOffset);
		storeBuffer.limit(limit);

		//System.out.printf("[DS] Storing %s in channel: bufRem=%d, bufPos=%d, dirtyOffset=%d, limit=%d, toWrite=%d, chanPos=%d\n",
		//		id, storeBuffer.remaining(), storeBuffer.position(), dirtyOffset, limit, limit-dirtyOffset, channel.position());

		int size = limit-dirtyOffset;

		assert channel.position() + size <= conf.dataBlockSizeBytes;

		while (bytesWritten < size) {
			bytesWritten += channel.write(storeBuffer);
		}

		dirtyOffset += bytesWritten;

		//System.out.printf("[DS] After storing %s in channel: written=%d, dirtyOffset=%d, writePos=%d, chanPos=%d\n",
		//		id, bytesWritten, dirtyOffset, writePos.get(), channel.position());

		return bytesWritten;
	}

	@Override
	protected synchronized void loadBlockOnNonPrimary(boolean loadFromPrimary) throws FileNotExistException, IOException {
		/* If never fetched or the last global-fetch has timed out, fetch from the global store.
		 * Otherwise, if the last primary-fetch has timed out, fetch from the primary node.
		 * Otherwise, don't fetch, data is still fresh.
		 */

		/**
		 * FIXME: The current implementation of this function is not ideal. Currently, an unfinished segment is reloaded
		 * from a remote source after a timeout. However, a segment should only be reloaded from a remote node if the
		 * read size exceeds the valid bytes in the loaded segment. In the current implementation, an unfinished
		 * data segment may get loaded from the remote node unnecessarily.
		 */

		if (isSegFull) { // Never load an already loaded block if the segment is full.
			//System.out.printf("[DS] Segment %s is full. Not loading the segment again.\n", id);
			return;
		}

		long now = System.currentTimeMillis();

		if (lastFetchTimeMs >= now - conf.dataSegmentFetchExpiryTimeoutMs) { // If the last fetch from the global store has expired
			return; // Throttle loading the block from the remote location
		}

		if (loadFromPrimary) {
			//System.out.println("[DS] Loading from the primary: " + id());
			loadBlockFromPrimary(); // Fetch data from the primary node
			//lastPrimaryFetchTimeMs = now;
			if (lastFetchTimeMs == 0) // Set to now if the global fetch has failed
				lastFetchTimeMs = now;
			return;
		}

		int pos = writePos.get();
		int offset = pos + ((DataSegmentID)id).segmentInBlock()* SEGMENT_SIZE_BYTES;
		int length = SEGMENT_SIZE_BYTES - pos;
		try {
			//System.out.println("[DS] Load from the global: " + id());

			loadFromGlobal(offset, length); // First try loading data from the global store
			lastFetchTimeMs = Long.MAX_VALUE; // Never expire data fetched from the global store.
			return;

			//TODO: If this block cannot be further modified, never expire the loaded data. For example, if it was the last segment of the block.
		} catch (FileNotExistException e) { //If the block is not in the global store yet
			//System.out.println("[DS] Not found in the global: " + id());
			lastFetchTimeMs = 0; // Failed to fetch from the global store
		}

		//System.out.println("[DS] Primary fetch expired or not found from the global: " + id());

		try {
			//System.out.println("[DS] Loading from the primary: " + id());
			loadBlockFromPrimary(); // Fetch data from the primary node
			//lastPrimaryFetchTimeMs = now;
			if (lastFetchTimeMs == 0) // Set to now if the global fetch has failed
				lastFetchTimeMs = now;
		} catch (FileNotExistException ke) { // If the file is not on the primary node, check again from the global store
			// Check again from the global store because the primary may have deleted the
			// block after copying to the global store
			//System.out.println("[DS] Not found on the primary, trying again from the global: " + id());
			loadFromGlobal(offset, length);
			lastFetchTimeMs = now;
			//lastPrimaryFetchTimeMs = 0;
		}
	}

	public int recordSize() {
		return recordSize;
	}

	public int segmentInBlock() {
		return ((DataSegmentID)id).segmentInBlock();
	}

	public long blockInFile() {
		return ((DataSegmentID)id).blockInFile();
	}

	public long fileID() {
		return ((DataSegmentID)id).inumber();
	}


	@Override
	public int sizeWhenSerialized() {
		return SEGMENT_SIZE_BYTES;
	}

	@Override
	public String toString(){
		return id().toString();
	}
}
