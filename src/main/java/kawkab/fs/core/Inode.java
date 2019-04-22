package kawkab.fs.core;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.atomic.AtomicLong;

import kawkab.fs.commons.Commons;
import kawkab.fs.commons.Configuration;
import kawkab.fs.core.exceptions.InsufficientResourcesException;
import kawkab.fs.core.exceptions.InvalidFileOffsetException;
import kawkab.fs.core.exceptions.KawkabException;
import kawkab.fs.core.exceptions.MaxFileSizeExceededException;

public final class Inode {
	//FIXME: fileSize can be internally staled. This is because append() is a two step process. 
	// The FileHandle first calls append and then calls updateSize to update the fileSize. Ideally, 
	// We should update the fileSize immediately after adding a new block. The fileSize is not being
	// updated immediately to allow concurrent reading and writing to the same dataBlock.

	private static final Configuration conf = Configuration.instance();
	
	private long inumber;
	private AtomicLong fileSize = new AtomicLong(0);
	private int recordSize = conf.recordSize; //Temporarily set to 1 until we implement reading/writing records
	
	private static Cache cache;
	private static LocalStoreManager localStore;
	
	private DataSegmentID segId;
	private DataSegment curSeg;
	private volatile SegmentTimer timer; // volatile so that the SegmentTimerQueue thread can read the most recent value
	private SegmentTimerQueue timerQ;
	
	public static final long MAXFILESIZE;
	static {
		MAXFILESIZE = conf.maxFileSizeBytes;//blocks * Constants.blockSegmentSizeBytes;
		
		cache = Cache.instance();
		localStore = LocalStoreManager.instance();
	}
	
	protected Inode(long inumber){
		this.inumber = inumber;
		timerQ = SegmentTimerQueue.instance();
	}
	
	/**
	 * Returns the ID of the segment that contains the given offset in file
	 * @throws IOException
	 */
	private BlockID getByFileOffset(long offsetInFile) throws IOException {
		long segmentInFile = DataSegment.segmentInFile(offsetInFile, recordSize);
		int segmentInBlock = DataSegment.segmentInBlock(segmentInFile);
		long blockInFile = DataSegment.blockInFile(segmentInFile);
		
		return new DataSegmentID(inumber, blockInFile, segmentInBlock);
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
			
			long segNumber = DataSegment.segmentInFile(curOffsetInFile, recordSize);
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
	
	public int appendBuffered(final byte[] data, int offset, final int length) throws MaxFileSizeExceededException, IOException, InterruptedException, KawkabException {
		int remaining = length; //Reaming size of data to append
		long fileSizeBuffered = this.fileSize.get(); // Current file size
		
		if (fileSizeBuffered + length > MAXFILESIZE) {
			throw new MaxFileSizeExceededException();
		}
		
		while (remaining > 0) {
			if (timer == null) { //If null, no need to synchronize because the SegmentTimerQueue thread will never synchronize with this timer
				if ((fileSizeBuffered % conf.dataBlockSizeBytes) == 0L) { // if the last block is full
					segId = createNewBlock(fileSizeBuffered);
				} else { // Otherwise the last segment was full or we are just starting without any segment at hand
					segId = getSegmentID(fileSizeBuffered);
				}
				
				curSeg = (DataSegment) cache.acquireBlock(segId);
				curSeg.initForAppend(fileSizeBuffered);
				timer = new SegmentTimer(segId, this);
			} else {
				synchronized (this) {
					if (timer == null || !timer.disableIfValid()) { // Disable the timer if it has not already expired. The condition is true if the timer has expired before it is disabled
						curSeg = (DataSegment) cache.acquireBlock(segId); // Acquire the segment again as the cache may have evicted the segment
						timer = new SegmentTimer(segId, this); // The previous timer cannot be reused because its state cannot be changed
					}
				}
			}
			
			try {
				int bytes = curSeg.append(data, offset, remaining, fileSizeBuffered);
				
				remaining -= bytes;
				offset += bytes;
				fileSizeBuffered += bytes;
			} catch (IOException e) {
				e.printStackTrace();
				throw new KawkabException(e);
			}
			
			localStore.store(curSeg);
			
			timer.update();
			
			timerQ.add(timer, inumber);
			
			if (curSeg.isFull()) { // If the current segment is full
				curSeg = null;
				segId = null;
				timer = null;
			}
		}
		
		fileSize.set(fileSizeBuffered);
		
		return length;
	}

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
			synchronized (this) {
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

	private DataSegmentID getSegmentID(long offsetInFile) {
		long segmentInFile = DataSegment.segmentInFile(offsetInFile, recordSize);
		long blockInFile = DataSegment.blockInFile(segmentInFile);
		int segmentInBlock = DataSegment.segmentInBlock(segmentInFile);
		
		return new DataSegmentID(inumber, blockInFile, segmentInBlock);
	}
	
	/**
	 * Append data at the end of the file
	 * @param data Data to append
	 * @param length Number of bytes to write from the data array
	 * @return number of bytes appended
	 * @throws InvalidFileOffsetException 
	 * @throws IOException 
	 * @throws KawkabException 
	 * @throws InterruptedException 
	 */
	public int append(final byte[] data, int offset, final int length) throws MaxFileSizeExceededException, 
				InvalidFileOffsetException, IOException, KawkabException, InterruptedException{
		int remaining = length;
		int appended = 0;
		long tmpFileSize = this.fileSize.get();
		
		if (tmpFileSize + length > MAXFILESIZE) {
			throw new MaxFileSizeExceededException();
		}
		
		BlockID segId = null;
		int curOffset = offset;
		
		//FIXME: What happens if an exception occurs? What append size is returned? We need to do some cleanup work
		//to rollback any data changes.
		
		while(remaining > 0) {
			int lastSegCapacity = conf.segmentSizeBytes - (int)(tmpFileSize % conf.segmentSizeBytes);
			
			//if (fileSize % Constants.dataBlockSizeBytes == 0) {
			if ((int)(tmpFileSize % conf.dataBlockSizeBytes) == 0) { // if the last block is full
				segId = createNewBlock(tmpFileSize);
				lastSegCapacity = conf.segmentSizeBytes;
			} else {
				segId = getByFileOffset(tmpFileSize);
			}
			
			int bytes;
			int toAppend = remaining <= lastSegCapacity ? remaining : lastSegCapacity;
			
			//TODO: Delete the newly created block if append fails. Also add condition in the DataBlock.createNewBlock()
			// to throw an exception if the file already exists.
			DataSegment seg = null;
			try {
				seg = (DataSegment)cache.acquireBlock(segId);
				bytes = seg.append(data, curOffset, toAppend, tmpFileSize);
			} finally {
				if (seg != null) {
					localStore.store(seg);
					cache.releaseBlock(seg.id());
				}
			}
			
			remaining -= bytes;
			curOffset += bytes;
			appended += bytes;
			tmpFileSize += bytes;
		}
		
		this.fileSize.addAndGet(appended);
		
		return appended;
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

		return Long.BYTES*2 + Integer.BYTES;
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
	 * the append function. Otherwise, we should make the append and close synchronized.
	 */
	void releaseBuffer() throws KawkabException {
		if (timer == null)
			return;

		if (timer.disableIfValid()) {
			cache.releaseBlock(segId);

			timer = null;
			curSeg = null;
			segId = null;

			return;
		}
	}
}
