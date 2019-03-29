package kawkab.fs.core;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
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
	private SegmentTimer timer;
	private SegmentTimerQueue timerQ;
	
	private WritableByteChannel channel;
	
	public static final long MAXFILESIZE;
	static {
		MAXFILESIZE = conf.maxFileSizeBytes;//blocks * Constants.blockSegmentSizeBytes;
		
		cache = Cache.instance();
		localStore = LocalStoreManager.instance();
	}
	
	protected Inode(long inumber/*, int recordSize*/){
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
	 * @throws InvalidFileOffsetException
	 * @throws IllegalArgumentException
	 * @throws IOException
	 * @throws KawkabException
	 * @throws InterruptedException
	 */
	public int read(final byte[] buffer, final int length, final long offsetInFile) throws InvalidFileOffsetException, 
					IllegalArgumentException, IOException, KawkabException, InterruptedException{
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
			//int offsetInSeg = DataSegment.offsetInSegment(curOffsetInFile, recordSize);
			//int canReadFromSeg = Constants.segmentSizeBytes - offsetInSeg;
			//int toRead = (int)(remaining >= canReadFromSeg ? canReadFromSeg : remaining);
			int bytes = 0;
			
			// System.out.println(String.format("Seg=%s, bufLen=%d, bufOffset=%d, toRead=%d, offInFile=%d, remInBuf=%d,dataRem=%d",
			//     curSegId.toString(), buffer.length, bufferOffset, toRead, curOffsetInFile, buffer.length-bufferOffset,remaining));
			
			assert bufferOffset+toRead <= buffer.length;
			
			DataSegment curSegment = null;
			try {
				curSegment = (DataSegment)cache.acquireBlock(curSegId);
				curSegment.loadBlock(); //The segment data might not be loaded when we get from the cache
				bytes = curSegment.read(buffer, bufferOffset, toRead, curOffsetInFile);
			} catch (InvalidFileOffsetException e) {
				e.printStackTrace();
				return bufferOffset;
			} finally {
				if (curSegment != null) {
					cache.releaseBlock(curSegment.id());
				}
			}
			
			bufferOffset += bytes;
			remaining -= bytes;
			curOffsetInFile += bytes;
		}
		
		return bufferOffset;
	}
	
	public int appendDirect(final byte[] data, int offset, final int length) throws MaxFileSizeExceededException, IOException, InterruptedException, KawkabException {
		int appended = 0;
		int remaining = length;
		long fileSizeBuffered = this.fileSize.get();
		
		if (fileSizeBuffered + length > MAXFILESIZE) {
			throw new MaxFileSizeExceededException();
		}
		
		while (remaining > 0) {
			if (channel == null) {
				DataSegmentID segId;
				if ((fileSizeBuffered % conf.dataBlockSizeBytes) == 0L) { // if the last block is full
					segId = createNewBlock(fileSizeBuffered);
				} else {
					segId = getSegmentID(fileSizeBuffered);
				}
				
				channel = Channels.newChannel(new BufferedOutputStream(new FileOutputStream(segId.localPath())));
			}
			
			try {
				ByteBuffer buffer = ByteBuffer.wrap(data, offset, length);
				int bytes = channel.write(buffer);
				
				remaining -= bytes;
				appended += bytes;
				fileSizeBuffered += bytes;
			} catch (IOException e) {
				e.printStackTrace();
				channel.close();
				channel = null;
				throw new KawkabException(e);
			}
			
			if ((fileSizeBuffered % conf.dataBlockSizeBytes) == 0L) {
				channel.close();
				channel = null;
			}
		}
		
		this.fileSize.set(fileSizeBuffered);
		
		return appended;
	}
	
	public int appendBuffered(final byte[] data, int offset, final int length) throws MaxFileSizeExceededException, IOException, InterruptedException, KawkabException {
		int appended = 0;
		int remaining = length;
		long fileSizeBuffered = this.fileSize.get();
		
		if (fileSizeBuffered + length > MAXFILESIZE) {
			throw new MaxFileSizeExceededException();
		}
		
		while (remaining > 0) {
			if (curSeg == null) {
				if ((fileSizeBuffered % conf.dataBlockSizeBytes) == 0L) { // if the last block is full
					segId = createNewBlock(fileSizeBuffered);
				} else {
					segId = getSegmentID(fileSizeBuffered);
				}
				
				timer = new SegmentTimer(segId);
				curSeg = (DataSegment) cache.acquireBlock(segId);
				curSeg.markLoaded();
			} else if (!timer.disable()) {
				timer = new SegmentTimer(segId);
				curSeg = (DataSegment) cache.acquireBlock(segId);
			}
			
			try {
				int bytes = curSeg.append(data, offset, remaining, fileSizeBuffered);
				
				remaining -= bytes;
				appended += bytes;
				fileSizeBuffered += bytes;
			} catch (IOException e) {
				e.printStackTrace();
				throw new KawkabException(e);
			}
			
			localStore.store(curSeg);
			
			timer.update();
			
			timerQ.submit(timer);
			
			if (fileSizeBuffered % conf.segmentSizeBytes == 0) {
				curSeg = null;
				segId = null;
			}
		}
		
		this.fileSize.set(fileSizeBuffered);
		
		return appended;
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
	 * @param curOffset Offset in the data array
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
		
		//long t = System.nanoTime();
		
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
					cache.releaseBlock(seg.id());
				}
			}
			
			remaining -= bytes;
			curOffset += bytes;
			appended += bytes;
			tmpFileSize += bytes;
			//dirty = true;
		}
		
		//long elapsed = (System.nanoTime() - t)/1000;
		//System.out.println("elaped: " + elapsed);
		
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
		
		// System.out.println(String.format("Creating blk for: %s, fileSize=%d", dsid, fileSize));
		
		localStore.createBlock(dsid);
		
		return dsid;
	}
	
	public long fileSize(){
		return fileSize.get();
	}
	
	void loadFrom(final ByteBuffer buffer) throws IOException {
		if (buffer.remaining() < conf.inodeSizeBytes) {
			throw new InsufficientResourcesException(String.format("Not enough bytes left in the buffer: "
					+ "Have %d, needed %d.",buffer.remaining(), conf.inodeSizeBytes));
		}
		
		inumber = buffer.getLong();
		fileSize.set(buffer.getLong());
		recordSize = buffer.getInt();
	}
	
	void loadFrom(final ReadableByteChannel channel) throws IOException {
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
		
		//if (fileSize.get() > 0)
		//	System.out.printf("Stored inode: %d -> %d\n", inumber, fileSize.get());
		
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
	
	public static int inodesSize(){
		//FIXME: Make it such that inodesBlockSizeBytes % inodeSizeBytes == 0
		//This can be achieved if size is rounded to 32, 64, 128, ...
		
		//Direct and indirect pointers + filesize + inumber + reserved.
		int size = (conf.directBlocksPerInode + 3)*16 + 8 + 0;
		size = size + (64 % size);
		
		return size; 
	}
	
	long inumber() {
		return inumber;
	}
}
