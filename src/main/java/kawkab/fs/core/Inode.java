package kawkab.fs.core;

import kawkab.fs.api.Record;
import kawkab.fs.commons.Commons;
import kawkab.fs.commons.Configuration;
<<<<<<< HEAD
import kawkab.fs.commons.FixedLenRecordUtils;
import kawkab.fs.core.exceptions.*;
import kawkab.fs.core.index.FileIndex;
=======
import kawkab.fs.core.exceptions.InsufficientResourcesException;
import kawkab.fs.core.exceptions.InvalidFileOffsetException;
import kawkab.fs.core.exceptions.KawkabException;
import kawkab.fs.core.exceptions.MaxFileSizeExceededException;
>>>>>>> batching

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.atomic.AtomicLong;

public final class Inode {
	//FIXME: fileSize can be internally staled. This is because append() is a two step process. 
	// The FileHandle first calls append and then calls updateSize to update the fileSize. Ideally, 
	// We should update the fileSize immediately after adding a new block. The fileSize is not being
	// updated immediately to allow concurrent reading and writing to the same dataBlock.
	
	private static final Configuration conf = Configuration.instance();
	
	private long inumber;
	private AtomicLong fileSize = new AtomicLong(0);
<<<<<<< HEAD
	private int recordSize; //Temporarily set to 1 until we implement reading/writing records
	private FileIndex index;
=======
	private int recordSize = 1; //Temporarily set to 1 until we implement reading/writing records
>>>>>>> batching
	
	private static Cache cache;
	private static LocalStoreManager localStore;
	
<<<<<<< HEAD
	private DataSegmentID segId;
	private DataSegment curSeg;
	private volatile SegmentTimer timer; // volatile so that the SegmentTimerQueue thread can read the most recent value
	private SegmentTimerQueue timerQ;
	private final Object timerLock = new Object();
=======
//	private DataSegmentID segId;
//	private DataSegment curSeg;
//	private volatile SegmentTimer timer; // volatile so that the SegmentTimerQueue thread can read the most recent value
	
	private volatile BufferedSegment acquiredSeg;
	//private SegmentTimerQueue timerQ;
	//private final Object timerLock = new Object();
>>>>>>> batching
	
	public static final long MAXFILESIZE;
	
	static {
		MAXFILESIZE = conf.maxFileSizeBytes;//blocks * Constants.blockSegmentSizeBytes;
		
		cache = Cache.instance();
		localStore = LocalStoreManager.instance();
	}
	
	protected Inode(long inumber, int recordSize) {
		this.inumber = inumber;
<<<<<<< HEAD
		this.recordSize = recordSize;
		timerQ = SegmentTimerQueue.instance();
=======
		//timerQ = SegmentTimerQueue.instance();
>>>>>>> batching
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
	
	/**
	 * Performs the read operation. Single writer and multiple readers are allowed to write and read concurrently.
	 * @return
	 * @throws IllegalArgumentException
	 * @throws InvalidFileOffsetException
	 * @throws IOException
	 * @throws KawkabException
	 */
	public boolean read(final Record dstRecord, final long key) throws
			RecordNotFoundException, IOException, KawkabException {
		
		long offsetInFile = index.offsetInFile(key);
		
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
	public boolean readRecordN(final Record dstRecord, final long recNum) throws
			RecordNotFoundException, IOException, KawkabException {
		
		long offsetInFile = (recNum-1L) * recordSize;
		
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
	
<<<<<<< HEAD
	public int appendBuffered(final Record record)
			throws IOException, InterruptedException, KawkabException {
		int length = record.size();
		
		long fileSizeBuffered = this.fileSize.get(); // Current file size
		
		if (fileSizeBuffered + length > MAXFILESIZE) {
			throw new MaxFileSizeExceededException();
		}
		
		if (timer == null) { //If null, no need to synchronize because the SegmentTimerQueue thread will never synchronize with this timer
			assert curSeg == null;
			assert segId == null;
			
			if ((FixedLenRecordUtils.offsetInBlock(fileSizeBuffered, recordSize) % conf.dataBlockSizeBytes) == 0L) { // if the last block is full
				segId = createNewBlock(fileSizeBuffered);
			} else { // Otherwise the last segment was full or we are just starting without any segment at hand
				segId = getSegmentID(fileSizeBuffered);
			}
			
			curSeg = (DataSegment) cache.acquireBlock(segId);
			curSeg.initForAppend(fileSizeBuffered, recordSize);
			timer = new SegmentTimer(segId, this);
		} else {
			synchronized (this) {
				if (timer == null || !timer.disableIfValid()) { // Disable the timer if it has not already expired. The condition is true if the timer has expired before it is disabled
					segId = getSegmentID(fileSizeBuffered);
					curSeg = (DataSegment) cache.acquireBlock(segId); // Acquire the segment again as the cache may have evicted the segment
					timer = new SegmentTimer(segId, this); // The previous timer cannot be reused because its state cannot be changed
				}
			}
		}
		
		assert curSeg.remaining() >= length;
		
		try {
			int bytes = curSeg.append(record.copyOutSrcBuffer(), fileSizeBuffered, recordSize);
			
			index.append(record.key(), fileSizeBuffered);
			
			fileSizeBuffered += bytes;
		} catch (IOException e) {
			throw new KawkabException(e);
		}
		
		if (curSeg.remaining() < record.size()) {
			curSeg.markAsFull();
		}
		
		if (curSeg.isFull()) { // If the current segment is full
			cache.releaseBlock(segId);
			curSeg = null;
			segId = null;
			timer = null;
		} else {
			timer.update();
			timerQ.add(timer);
		}
		
		fileSize.set(fileSizeBuffered);
		
		return length;
	}
	
	public int appendBuffered(final byte[] data, int offset, final int length)
=======
	//public static TimeLog tlog1 = new TimeLog(TimeLog.TimeLogUnit.NANOS, "ab all");
	public synchronized int appendBuffered(final byte[] data, int offset, final int length) //Syncrhonized with close() due to acquiredSeg
>>>>>>> batching
			throws MaxFileSizeExceededException, IOException, InterruptedException, KawkabException {
		int remaining = length; //Reaming size of data to append
		long fileSizeBuffered = this.fileSize.get(); // Current file size
		
		if (fileSizeBuffered + length > MAXFILESIZE) {
			throw new MaxFileSizeExceededException();
		}
		
		
		while (remaining > 0) {
			if (acquiredSeg == null) { //If null, no need to synchronize because the SegmentTimerQueue thread will never synchronize with this timer
				DataSegmentID segId;
				if ((fileSizeBuffered % conf.dataBlockSizeBytes) == 0L) { // if the last block is full
					segId = createNewBlock(fileSizeBuffered);
				} else { // Otherwise the last segment was full or we are just starting without any segment at hand
					segId = getSegmentID(fileSizeBuffered);
				}
				
<<<<<<< HEAD
				
				curSeg = (DataSegment) cache.acquireBlock(segId);
				curSeg.initForAppend(fileSizeBuffered, recordSize);
				timer = new SegmentTimer(segId, this);
			} else {
				synchronized (timerLock) {
					if (timer == null || !timer.disableIfValid()) { // Disable the timer if it has not already expired. The condition is true if the timer has expired before it is disabled
						segId = getSegmentID(fileSizeBuffered);
						curSeg = (DataSegment) cache.acquireBlock(segId); // Acquire the segment again as the cache may have evicted the segment
						timer = new SegmentTimer(segId, this); // The previous timer cannot be reused because its state cannot be changed
					}
=======
				acquiredSeg = new BufferedSegment(segId, fileSizeBuffered);
			} else {
				if (!acquiredSeg.freeze()) {	// Tries to freeze the bufferedSegment so that the acquired segement cannot be returned back to the cache.The condition is true if the timer has expired before it is disabled
					DataSegmentID segId = getSegmentID(fileSizeBuffered);	// Acquire the segment again as the cache may have evicted the segment. //
																			// The previous timer cannot be reused because its state cannot be changed
					acquiredSeg = new BufferedSegment(segId, fileSizeBuffered);
>>>>>>> batching
				}
			}
			
			try {
				
				int bytes = acquiredSeg.append(data, offset, remaining, fileSizeBuffered);
				
				remaining -= bytes;
				offset += bytes;
				fileSizeBuffered += bytes;
			} catch (IOException e) {
				throw new KawkabException(e);
			}
			
<<<<<<< HEAD
			if (curSeg.isFull()) { // If the current segment is full
				cache.releaseBlock(segId);
				curSeg = null;
				segId = null;
				timer = null;
			} else {
				timer.update();
				timerQ.add(timer);
=======
			//tlog1.start();
			if (acquiredSeg.isFull()) {	// If the current segment is full, we don't need to keep the segment as the segment
										// is now immutable. We can return the segment to the cache.
				
				acquiredSeg.release(); 	// Don't add back in the queue as the timer will not be reused again. The segmentTimerQueue
										// thread will just throw way the timer because it is disabled.
				acquiredSeg = null;
			} else {
				acquiredSeg.unfreeze();
>>>>>>> batching
			}
		}
		
		fileSize.set(fileSizeBuffered);
		
		return length;
	}

<<<<<<< HEAD
	/**
	 * Releases the dataSegment if the timer has expired, while synchronizing with the append thread in the
	 * <code>appendBuffered()</code> function.
	 * @param expiredTimer Expired SegmemtTimer
	 * @param expiredSegId ID of the data segment that should be returned to the cache for reference counting
	 */
	void onTimerExpiry(SegmentTimer expiredTimer, DataSegmentID expiredSegId) throws KawkabException {
		assert expiredTimer != null;

		// FIXME: Need a review about synchronization
		// We don't need to synchronize because (a) the timer expiry thread can reach here only when the timer has
		// expired, (b) the appender can either read the stale value or find the timer field to be null. In both of the
		// cases, the appender creates a new timer object and acquire the data segment from the cache. So it is safe to
		// update timer, curSeg, and segId fields to null without synchronizing with the appender.
		// However, this thread, which is the SegmentTimerQueue thread, has to read the most recent value of the
		// timer object. Therefore, the timer object is volatile to ensure that it doesn't red the staled value.

		if (expiredTimer == timer) {// Checking the references because the same timers should have same reference
			synchronized (timerLock) {
				if (expiredTimer == timer) { // Have to check again so that this thread and the appender synchronize on the same object
					curSeg = null;
					segId = null;
					timer = null; 	// Note: It is important to set timer to null after setting the segId to null.
									// Otherwise, the "if (timer == null)" condition in the appendBuffered function may fail.
				}
			}
		}

		cache.releaseBlock(expiredSegId);
	}

=======
>>>>>>> batching
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
		
		int written = buffer.position() - start;
		return written;
	}

	/**
	 * This function must not be concurrent to the append function. So this should be called from the same thread as
	 * the append function. Otherwise, we should synchronize the caller and the appender in append and close functions.
	 */
	synchronized void releaseBuffer() throws KawkabException { //Syncrhonized with appendBuffered() due to acquiredSeg
		//tlog.printStats("DS.append,ls.store");
<<<<<<< HEAD
		
		synchronized (timerLock) { // Synchronize with the SegmentTimeQueue.
			if (timer == null) {
				assert curSeg == null;
				assert segId == null;
				return;
			}

			if (timer.disableIfValid()) {
				cache.releaseBlock(segId);
				
				timer = null;
				curSeg = null;
				segId = null;
				
				return;
			}
=======
		if (acquiredSeg != null) {
			acquiredSeg.freeze();
			acquiredSeg.release();
			acquiredSeg = null;
>>>>>>> batching
		}
	}
}
