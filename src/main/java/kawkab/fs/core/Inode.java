package kawkab.fs.core;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import kawkab.fs.commons.Commons;
import kawkab.fs.commons.Constants;
import kawkab.fs.core.exceptions.IndexBlockFullException;
import kawkab.fs.core.exceptions.InsufficientResourcesException;
import kawkab.fs.core.exceptions.InvalidFileOffsetException;
import kawkab.fs.core.exceptions.KawkabException;
import kawkab.fs.core.exceptions.MaxFileSizeExceededException;

public final class Inode {
	//FIXME: fileSize can be internally staled. This is because append() is a two step process. 
	// The FileHandle first calls append and then calls updateSize to update the fileSize. Ideally, 
	// We should update the fileSize immediately after adding a new block. The fileSize is not being
	// updated immediately to allow concurrent reading and writing to the same dataBlock.
	
	private long inumber;
	private volatile long fileSize;
	private int recordSize = 1; //Temporarily set to 1 until we implement reading/writing records
	private boolean dirty; //not saved persistently
	private static Cache cache;
	
	public static final long MAXFILESIZE;
	static {
		MAXFILESIZE = Long.MAX_VALUE;//blocks * Constants.blockSegmentSizeBytes;
		
		try {
			cache = Cache.instance();
		} catch (IOException e) {
			e.printStackTrace(); //FIXME: Handle exception properly
		}
	}
	
	protected Inode(long inumber/*, int recordSize*/){
		this.inumber = inumber;
		//this.recordSize = recordSize;
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
		
		if (offsetInFile + length > fileSize)
			throw new IllegalArgumentException(String.format("File offset + read length is greater "
					+ "than file size: %d + %d > %d", offsetInFile,length,fileSize));
		
		int bufferOffset = 0;
		int remaining = length;
		long curOffsetInFile = offsetInFile;
		
		while(remaining > 0 && curOffsetInFile < fileSize) {
			//System.out.println("  Read at offset: " + offsetInFile);
			BlockID curSegId = getByFileOffset(curOffsetInFile);
			
			//System.out.println("Reading block at offset " + offsetInFile + ": " + curBlkUuid.key);
			
			long segNumber = DataSegment.segmentInFile(curOffsetInFile, recordSize);
			long nextSegStart = (segNumber + 1) * Constants.segmentSizeBytes;
			int toRead = (int)(curOffsetInFile+remaining <= nextSegStart ? remaining : nextSegStart - curOffsetInFile);
			int bytes = 0;
			
			//System.out.println(String.format("Seg=%d, bufOffset=%d, toRead=%d, offInFile=%d",segNumber, bufferOffset, toRead, curOffsetInFile));
			
			DataSegment curSegment = null;
			try {
				curSegment = (DataSegment)cache.acquireBlock(curSegId, false);
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
	
	DataSegment getByTime(long timestamp, boolean blockBeforeTime){
		return null;
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
		long fileSize = this.fileSize;
		
		BlockID lastSegID = null;
		int curOffset = offset;
		
		//FIXME: What happens if an exception occurs? What append size is returned? We need to do some cleanup work
		//to rollback any data changes.
		
		//long t = System.nanoTime();
		
		while(remaining > 0) {
			int lastSegCapacity = Constants.segmentSizeBytes - (int)(fileSize % Constants.segmentSizeBytes);
			
			//if (fileSize % Constants.dataBlockSizeBytes == 0) {
			if ((int)(fileSize % Constants.dataBlockSizeBytes) == 0) { // if last block is full
				try {
					lastSegID = createNewBlock(fileSize);
					lastSegCapacity = Constants.segmentSizeBytes;
				} catch (IndexBlockFullException e) {
					e.printStackTrace();
					return appended;
				}
			}
			
			lastSegID = getByFileOffset(fileSize);
			
			int bytes;
			int toAppend = remaining <= lastSegCapacity ? remaining : lastSegCapacity;
			
			//TODO: Delete the newly created block if append fails. Also add condition in the DataBlock.createNewBlock()
			// to throw an exception if the file already exists.
			DataSegment seg = null;
			try {
				seg = (DataSegment)cache.acquireBlock(lastSegID, false);
				bytes = seg.append(data, curOffset, toAppend, fileSize);
			} finally {
				if (seg != null) {
					cache.releaseBlock(seg.id());
				}
			}
			
			remaining -= bytes;
			curOffset += bytes;
			appended += bytes;
			fileSize += bytes;
			//dirty = true;
		}
		
		//long elapsed = (System.nanoTime() - t)/1000;
		//System.out.println("elaped: " + elapsed);
		
		return appended;
	}
	
	/**
	 * The caller must lock the InodesBlock before calling this function.
	 * @param appendLength The number of bytes by which the file size is being increased.
	 */
	public void updateSize(final int appendLength) {
		//FIXME: This function should be part of the append function and should be called atomically with append.
		fileSize += appendLength;
		dirty = true;
	}
	
	/**
	 * Creates a new block in the virtual file.
	 * @param fileSize The current fileSize during the last append operation. Note that the
	 * fileSize instance variable is updated as the last step in the append operation.
	 * @return Returns the ID of the first segment in the block.
	 * @throws MaxFileSizeExceededException
	 * @throws IndexBlockFullException
	 * @throws InvalidFileOffsetException
	 * @throws IOException
	 * @throws InterruptedException 
	 * @throws KawkabException 
	 */
	BlockID createNewBlock(final long fileSize) throws MaxFileSizeExceededException, IndexBlockFullException, InvalidFileOffsetException, IOException, InterruptedException, KawkabException{
		//long blocksCount = (long)Math.ceil(1.0*fileSize / Constants.dataBlockSizeBytes);
		if (fileSize + Constants.segmentSizeBytes > MAXFILESIZE){
			throw new MaxFileSizeExceededException();
		}
		
		//long blockNumber = fileSize/Constants.blockSegmentSizeBytes; //Zero based block number;
		
		long segmentInFile = DataSegment.segmentInFile(fileSize, recordSize);
		int segmentInBlock = DataSegment.segmentInBlock(segmentInFile);
		long blockInFile = DataSegment.blockInFile(segmentInFile);
		
		assert segmentInBlock == 0;
		
		DataSegmentID dataSegmentID = new DataSegmentID(inumber, blockInFile, segmentInBlock);
		//DataSegment block = new DataSegment(dataSegmentID);
		//cache.createBlock(block);
		try {
			cache.acquireBlock(dataSegmentID, true);
		} finally {
			cache.releaseBlock(dataSegmentID);
		}
		
		//System.out.println(String.format("Created blk: %d, fileSize=%d", blockNumber, fileSize));
		
		return dataSegmentID;
	}
	
	public long fileSize(){
		return fileSize;
	}
	
	void loadFrom(final ByteBuffer buffer) throws IOException {
		if (buffer.remaining() < Constants.inodeSizeBytes) {
			throw new InsufficientResourcesException(String.format("Not enough bytes left in the buffer: "
					+ "Have %d, needed %d.",buffer.remaining(), Constants.inodeSizeBytes));
		}
		
		inumber = buffer.getLong();
		fileSize = buffer.getLong();
		recordSize = buffer.getInt();
	}
	
	void loadFrom(final ReadableByteChannel channel) throws IOException {
		int inodeSizeBytes = Constants.inodeSizeBytes;
		ByteBuffer buffer = ByteBuffer.allocate(inodeSizeBytes);
		
		int bytesRead = Commons.readFrom(channel, buffer);
		
		if (bytesRead < inodeSizeBytes) {
			throw new InsufficientResourcesException(String.format("Full block is not loaded. Loaded "
					+ "%d bytes out of %d.",bytesRead,inodeSizeBytes));
		}
		
		buffer.flip();
		inumber = buffer.getLong();
		fileSize = buffer.getLong();
		recordSize = buffer.getInt();
	}
	
	/*
	 * Serializes the inode in the channel
	 * @return Number of bytes written in the channel
	 */
	int storeTo(final WritableByteChannel channel) throws IOException {
		ByteBuffer buffer = ByteBuffer.allocate(Constants.inodeSizeBytes);
		storeTo(buffer);
		buffer.flip();
		buffer.limit(buffer.capacity());
		
		int bytesWritten = Commons.writeTo(channel, buffer);
		
		if (bytesWritten < Constants.inodeSizeBytes) {
			throw new InsufficientResourcesException(String.format("Full block is not strored. Stored "
					+ "%d bytes out of %d.",bytesWritten, Constants.inodeSizeBytes));
		}
		
		return bytesWritten;
	}
	
	int storeTo(ByteBuffer buffer) {
		int start = buffer.position();
		
		buffer.putLong(inumber);
		buffer.putLong(fileSize);
		buffer.putInt(recordSize);
		
		int written = buffer.position() - start;
		return written;
	}
	
	public static int inodesSize(){
		//FIXME: Make it such that inodesBlockSizeBytes % inodeSizeBytes == 0
		//This can be achieved if size is rounded to 32, 64, 128, ...
		
		//Direct and indirect pointers + filesize + inumber + reserved.
		int size = (Constants.directBlocksPerInode + 3)*16 + 8 + 0;
		size = size + (64 % size);
		
		return size; 
	}
	
	boolean dirty(){
		return dirty;
	}
	
	void clear(){
		dirty = false;
	}
}
