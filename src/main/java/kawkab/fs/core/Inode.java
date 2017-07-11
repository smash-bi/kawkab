package kawkab.fs.core;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;

import kawkab.fs.commons.Commons;
import kawkab.fs.commons.Constants;
import kawkab.fs.core.exceptions.IndexBlockFullException;
import kawkab.fs.core.exceptions.InsufficientResourcesException;
import kawkab.fs.core.exceptions.InvalidArgumentsException;
import kawkab.fs.core.exceptions.InvalidFileOffsetException;
import kawkab.fs.core.exceptions.MaxFileSizeExceededException;

public class Inode {
	//FIXME: fileSize can be internally staled. This is because append() is a two step process. 
	// The FileHandle first calls append and then calls updateSize to update the fileSize. Ideally, 
	// We should update the fileSize immediately after adding a new block. The fileSize is not being
	// updated immediately to allow concurrent reading and writing to the same dataBlock.
	private long fileSize; 
	private long inumber;
	//private BlockID directBlockUuid[];
	//private BlockID indirectBlockUuid;
	//private BlockID doubleIndirectBlockUuid;
	//private BlockID tripleIndirectBlockUuid;
	//private BlockID lastBlockUuid;
	private boolean dirty; //not saved persistently
	//private long blocksCount; //not saved persistently; blocksCount should be recalculated in fromBuffer().
	private Cache cache = Cache.instance();
	
	public static final long maxFileSize;
	static {
		//Calculate the maximum number of blocks that a file can span.
		long blocks = Constants.directBlocksPerInode;
		for(int i=1; i<=3; i++){ //Add the number of blocks for each index levels of 1, 2, and 3.
			blocks += Commons.maxBlocksCount(i);
		}
		maxFileSize = blocks * Constants.dataBlockSizeBytes;
	}
	
	//The constructor is private to disable creating the object externally.
	Inode(long inumber){
		this.inumber = inumber;
		
		//directBlockUuid = new BlockID[Constants.directBlocksPerInode];
		//for (int i=0; i<directBlockUuid.length; i++){
		//	directBlockUuid[i] = new BlockID(0, 0, DataBlock.name(0, 0), BlockType.DataBlock);
		//}
		
		//indirectBlockUuid = new BlockID(0, 0, DataBlock.name(0, 0), BlockType.DataBlock);
		//doubleIndirectBlockUuid = new BlockID(0, 0, DataBlock.name(0, 0), BlockType.DataBlock);
		//tripleIndirectBlockUuid = new BlockID(0, 0, DataBlock.name(0, 0), BlockType.DataBlock);
		//lastBlockUuid = new BlockID(0, 0, DataBlock.name(0, 0), BlockType.DataBlock);
	}
	
	BlockID getByFileOffset(long offsetInFile) throws InvalidFileOffsetException, IOException {
		if (offsetInFile < 0 || offsetInFile > fileSize) {
			throw new InvalidFileOffsetException(String.format("Invalid file offset %d. File size is %d bytes.",offsetInFile,fileSize));
		}
		
		long blockNumber = offsetInFile/Constants.dataBlockSizeBytes;
		
		//long blockIdx = offsetInFile / Constants.dataBlockSizeBytes;
		//long indirectBlocksLimit = Constants.directBlocksPerInode + Commons.maxBlocksCount(1);
		//long doubleIndirectBlocksLimit = indirectBlocksLimit + Commons.maxBlocksCount(2);
		
		//System.out.println(String.format("Block=%d, offset=%d", blockIdx, 
		//		offsetInFile, indirectBlocksLimit, doubleIndirectBlocksLimit));
		
		/*if (blockIdx < Constants.directBlocksPerInode){
			return directBlockUuid[(int)blockIdx];
		} else if (blockIdx < indirectBlocksLimit){
			IndexBlock indirectBlock = new IndexBlock(indirectBlockUuid, 1);
			return indirectBlock.getBlockIDByByte(offsetInFile);
		} else if (blockIdx < doubleIndirectBlocksLimit){
			IndexBlock doubleIndirectBlock = new IndexBlock(doubleIndirectBlockUuid, 2);
			return doubleIndirectBlock.getBlockIDByByte(offsetInFile);
		}
		
		IndexBlock tripleIndirectBlock = new IndexBlock(tripleIndirectBlockUuid, 3);
		return tripleIndirectBlock.getBlockIDByByte(offsetInFile);*/
		
		return DataBlock.newID(inumber, blockNumber);
	}
	
	public int read(byte[] buffer, int length, long offsetInFile) throws InvalidFileOffsetException, InvalidArgumentsException, IOException{
		//TODO: Check for input bounds
		if (length <= 0)
			return 0;
		
		if (offsetInFile + length > fileSize)
			throw new InvalidArgumentsException(String.format("File offset + read length is greater "
					+ "than file size: %d + %d > %d", offsetInFile,length,fileSize));
		
		int bufferOffset = 0;
		int remaining = length;
		
		while(remaining > 0 && offsetInFile < fileSize) {
			//System.out.println("  Read at offset: " + offsetInFile);
			BlockID curBlkUuid = getByFileOffset(offsetInFile);
			
			//System.out.println("Reading block at offset " + offsetInFile + ": " + curBlkUuid.key);
			
			long blockNumber = offsetInFile/Constants.dataBlockSizeBytes;
			long nextBlockStart = (blockNumber + 1) * Constants.dataBlockSizeBytes;
			int toRead = (int)(offsetInFile+remaining <= nextBlockStart ? remaining : nextBlockStart - offsetInFile);
			int bytes = 0;
			
			DataBlock currentBlock = null;
			try {
				currentBlock = (DataBlock)cache.acquireBlock(curBlkUuid);
				bytes = currentBlock.read(buffer, bufferOffset, toRead, offsetInFile);
			} catch (InvalidFileOffsetException e) {
				e.printStackTrace();
				return bufferOffset;
			} finally {
				if (currentBlock != null) {
					cache.releaseBlock(currentBlock.id());
				}
			}
			
			bufferOffset += bytes;
			remaining -= bytes;
			offsetInFile += bytes;
		}
		
		return bufferOffset;
	}
	
	DataBlock getByTime(long timestamp, boolean blockBeforeTime){
		return null;
	}
	
	/**
	 * Append data at the end of the file
	 * @param data Data to append
	 * @param offset Offset in the data array
	 * @param length Number of bytes to write from the data array
	 * @return number of bytes appended
	 * @throws InvalidFileOffsetException 
	 * @throws IOException 
	 */
	public int append(byte[] data, int offset, int length) throws MaxFileSizeExceededException, InvalidFileOffsetException, IOException{
		int remaining = length;
		int appended = 0;
		long fileSize = this.fileSize;
		
		BlockID lastBlockID = null;
		
		//long t = System.nanoTime();
		
		while(remaining > 0){
			int lastBlkCapacity = Constants.dataBlockSizeBytes - (int)(fileSize % Constants.dataBlockSizeBytes);
			
			//if (fileSize % Constants.dataBlockSizeBytes == 0) {
			if (lastBlkCapacity == Constants.dataBlockSizeBytes) { // if last block is full
				try {
					lastBlockID = createNewBlock(fileSize);
					lastBlkCapacity = Constants.dataBlockSizeBytes;
				} catch (IndexBlockFullException e) {
					e.printStackTrace();
					return appended;
				}
			} else {
				lastBlockID = getByFileOffset(this.fileSize);
			}
			
			int bytes;
			int toAppend = remaining <= lastBlkCapacity ? remaining : lastBlkCapacity;
			
			//TODO: Delete the newly created block if append fails. Also add condition in the DataBlock.createNewBlock()
			// to throw an exception if the file already exists.
			DataBlock block = null;
			try {
				block = (DataBlock)cache.acquireBlock(lastBlockID);
				bytes = block.append(data, offset, toAppend, fileSize);
			} finally {
				if (block != null) {
					cache.releaseBlock(block.id());
				}
			}
			
			remaining -= bytes;
			offset += bytes;
			appended += bytes;
			fileSize += bytes;
			//dirty = true;
		}
		
		//long elapsed = (System.nanoTime() - t)/1000;
		//System.out.println("elaped: " + elapsed);
		
		return appended;
	}
	
	/**
	 * The caller should get a lock on the inodesBlock before calling this function.
	 * @param appendLength The number of bytes by which the file size is being increased.
	 */
	public void updateSize(int appendLength){
		fileSize += appendLength;
		dirty = true;
	}
	
	/**
	 * @param fileSize The current fileSize during the last append operation. Note that the
	 * fileSize instance variable is updated as the last step in the append operation.
	 * @return Returns the newly created data block.
	 * @throws MaxFileSizeExceededException
	 * @throws IndexBlockFullException
	 * @throws InvalidFileOffsetException
	 * @throws IOException
	 */
	BlockID createNewBlock(final long fileSize) throws MaxFileSizeExceededException, IndexBlockFullException, InvalidFileOffsetException, IOException{
		//long blocksCount = (long)Math.ceil(1.0*fileSize / Constants.dataBlockSizeBytes);
		if (fileSize + Constants.dataBlockSizeBytes > maxFileSize){
			throw new MaxFileSizeExceededException();
		}
		
		long blockNumber = fileSize/Constants.dataBlockSizeBytes; //Zero based block number;
		BlockID dataBlockID = DataBlock.newID(inumber, blockNumber);
		DataBlock block = new DataBlock(dataBlockID);
		cache.createBlock(block);
		//BlockID dataBlockID = DataBlock.createNewBlock(inumber, blockNumber);
		
		//System.out.println(String.format("Created blk: %d, fileSize=%d", blockNumber, fileSize));
		
		return dataBlockID;
		
		//-------------------------------------------------------------------------
		//FIXME: This is for debugging only. 
		//try ( DataBlock dataBlock = (DataBlock)(cache.acquireBlock(dataBlockID))) {
		//	dataBlock.blockNumber = blocksCount+1;
		//}
		//-------------------------------------------------------------------------
		
		
		/*long indirectBlocksLimit = Constants.directBlocksPerInode + Commons.maxBlocksCount(1);
		long doubleIndirectBlocksLimit = indirectBlocksLimit + Commons.maxBlocksCount(2);
		long tripleIndirectBlocksLimit = doubleIndirectBlocksLimit + Commons.maxBlocksCount(3);
		
		if (blocksCount < Constants.directBlocksPerInode) {
			//System.out.println("1. Adding direct block "+directBlocksCreated);
			directBlockUuid[(int)blocksCount] = dataBlockID;
		} else {
			IndexBlock indBlock = null;
			
			if (blocksCount < indirectBlocksLimit) {
				//System.out.println("1. Adding indirect block "+blocksCount);
				if (indirectBlockUuid.uuidHigh == 0 && indirectBlockUuid.uuidLow == 0) {
					//DataBlock indDataBlock = cache.newDataBlock();
					BlockID indDataBlockID = DataBlock.createNewBlock();
					indBlock = new IndexBlock(indDataBlockID, 1); //IndexBlock is like a wrapper on DataBlock
					indirectBlockUuid = indBlock.uuid();
					System.out.println("\t\tCreated indirect block: " + Commons.uuidToString(indirectBlockUuid.uuidHigh, indirectBlockUuid.uuidLow));
				} else {
					indBlock = new IndexBlock(indirectBlockUuid, 1);
				}
			} else if (blocksCount < doubleIndirectBlocksLimit){
				if (doubleIndirectBlockUuid.uuidHigh == 0 && doubleIndirectBlockUuid.uuidLow == 0) {
					//System.out.println("1. Adding doubleIndirect block "+blocksCount);
					//DataBlock indDataBlock = cache.newDataBlock();
					BlockID indDataBlockID = DataBlock.createNewBlock();
					indBlock = new IndexBlock(indDataBlockID, 2);
					doubleIndirectBlockUuid = indBlock.uuid();
					System.out.println("\t\tCreated double indirect block.");
				} else {
					indBlock = new IndexBlock(doubleIndirectBlockUuid, 2);
				}
				
			} else if (blocksCount < tripleIndirectBlocksLimit){
				if (tripleIndirectBlockUuid.uuidHigh == 0 && tripleIndirectBlockUuid.uuidLow == 0) {
					//System.out.println("1. Adding tripleIndirect block "+blocksCount);
					//DataBlock indDataBlock = cache.newDataBlock();
					BlockID indDataBlockID = DataBlock.createNewBlock();
					indBlock = new IndexBlock(indDataBlockID, 3);
					tripleIndirectBlockUuid = indBlock.uuid();
					System.out.println("\t\tCreated triple indirect block.");
				} else {
					indBlock = new IndexBlock(tripleIndirectBlockUuid, 3);
				}
			} else {
				throw new MaxFileSizeExceededException();
			}
			
			indBlock.addBlock(dataBlockID, blocksCount);
		}*/
		
		//blocksCount++;
		//lastBlockUuid = dataBlockID;
	}
	
	public long fileSize(){
		return fileSize;
	}
	
	/*public long id(){
		return inumber;
	}*/
	
	void loadFrom(ByteChannel channel) throws IOException {
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
	}
	
	void storeTo(ByteChannel channel) throws IOException {
		ByteBuffer buffer = ByteBuffer.allocate(Constants.inodeSizeBytes);
		buffer.putLong(inumber);
		buffer.putLong(fileSize);
		buffer.flip();
		buffer.limit(buffer.capacity());
		
		int bytesWritten = Commons.writeTo(channel, buffer);
		
		if (bytesWritten < Constants.inodeSizeBytes) {
			throw new InsufficientResourcesException(String.format("Full block is not strored. Stored "
					+ "%d bytes out of %d.",bytesWritten, Constants.inodeSizeBytes));
		}
		
	}
	
	
	/*static Inode fromBuffer(ByteBuffer buffer) throws InsufficientResourcesException{
		int inodeSize = Constants.inodeSizeBytes;
		if (buffer.remaining() < inodeSize)
			throw new InsufficientResourcesException(String.format("Buffer has less bytes remaining: "
					+ "%d bytes are remaining, %d bytes are required.",buffer.remaining(),inodeSize));
		
		if (buffer.remaining() < Constants.inodeSizeBytes)
			throw new BufferUnderflowException();
		
		int initPosition = buffer.position();
		
		Inode inode = new Inode(0);
		inode.inumber = buffer.getLong();
		inode.fileSize = buffer.getLong();
		
		int dataLength = buffer.position() - initPosition;
		int padLength = Constants.inodeSizeBytes - dataLength;
		buffer.position(buffer.position()+padLength);
		
		return inode;
	}*/
	
	/*
	 * We need a way to ensure that the concurrent threads do not update this Inode while the block
	 * is being written to the buffer.
	 */
	/*void toBuffer(ByteBuffer buffer) throws InsufficientResourcesException{
		int inodeSize = Constants.inodeSizeBytes;
		if (buffer.capacity() < inodeSize)
			throw new InsufficientResourcesException(String.format("Buffer capacity is less than "
					+ "required: Capacity = %d bytes, required = %d bytes.",buffer.capacity(),inodeSize));
		
		int initPosition = buffer.position();
		
		buffer.putLong(inumber);
		buffer.putLong(fileSize);
		
		int dataLength = buffer.position() - initPosition;
		int padLength = Math.max(0, Constants.inodeSizeBytes - dataLength);
		
		assert dataLength + padLength == Constants.inodeSizeBytes;
		
		if (padLength > 0) {
			byte[] padding = new byte[padLength];
			buffer.put(padding);
		}
	}*/
	
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
