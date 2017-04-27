package kawkab.fs.core;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

import kawkab.fs.commons.Commons;
import kawkab.fs.commons.Constants;
import kawkab.fs.core.Block.BlockType;
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
	private BlockID directBlockUuid[];
	private BlockID indirectBlockUuid;
	private BlockID doubleIndirectBlockUuid;
	private BlockID tripleIndirectBlockUuid;
	private BlockID lastBlockUuid;
	private boolean dirty; //not saved persistently
	private long blocksCount; //not saved persistently; blocksCount should be recalculated in fromBuffer().
	
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
	Inode(){
		directBlockUuid = new BlockID[Constants.directBlocksPerInode];
		for (int i=0; i<directBlockUuid.length; i++){
			directBlockUuid[i] = new BlockID(0, 0, DataBlock.name(0, 0), BlockType.DataBlock);
		}
		
		indirectBlockUuid = new BlockID(0, 0, DataBlock.name(0, 0), BlockType.DataBlock);
		doubleIndirectBlockUuid = new BlockID(0, 0, DataBlock.name(0, 0), BlockType.DataBlock);
		tripleIndirectBlockUuid = new BlockID(0, 0, DataBlock.name(0, 0), BlockType.DataBlock);
		lastBlockUuid = new BlockID(0, 0, DataBlock.name(0, 0), BlockType.DataBlock);
	}
	
	BlockID getByFileOffset(long offsetInFile) throws InvalidFileOffsetException {
		if (offsetInFile < 0 || offsetInFile > fileSize) {
			throw new InvalidFileOffsetException(String.format("Invalid file offset %d. File size is %d bytes.",offsetInFile,fileSize));
		}
		
		long blockIdx = offsetInFile / Constants.dataBlockSizeBytes;
		long indirectBlocksLimit = Constants.directBlocksPerInode + Commons.maxBlocksCount(1);
		long doubleIndirectBlocksLimit = indirectBlocksLimit + Commons.maxBlocksCount(2);
		
		//System.out.println(String.format("Block=%d, offset=%d", blockIdx, 
		//		offsetInFile, indirectBlocksLimit, doubleIndirectBlocksLimit));
		
		if (blockIdx < Constants.directBlocksPerInode){
			return directBlockUuid[(int)blockIdx];
		} else if (blockIdx < indirectBlocksLimit){
			IndexBlock indirectBlock = new IndexBlock(indirectBlockUuid, 1);
			return indirectBlock.getBlockIDByByte(offsetInFile);
		} else if (blockIdx < doubleIndirectBlocksLimit){
			IndexBlock doubleIndirectBlock = new IndexBlock(doubleIndirectBlockUuid, 2);
			return doubleIndirectBlock.getBlockIDByByte(offsetInFile);
		}
		
		IndexBlock tripleIndirectBlock = new IndexBlock(tripleIndirectBlockUuid, 3);
		return tripleIndirectBlock.getBlockIDByByte(offsetInFile);
	}
	
	public int read(byte[] buffer, int length, long offsetInFile) throws InvalidFileOffsetException, InvalidArgumentsException{
		//TODO: Check for input bounds
		if (length <= 0)
			return 0;
		
		if (offsetInFile + length > fileSize)
			throw new InvalidArgumentsException(String.format("File offset + read length is greater "
					+ "than file size: %d + %d > %d", offsetInFile,length,fileSize));
		
		Cache cache = Cache.instance();
		
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
			try(DataBlock currentBlock = (DataBlock)cache.acquireBlock(curBlkUuid)) {
				try {
					//System.out.println("  Reading block " + Commons.uuidToString(curBlkUuidHigh, curBlkUuidLow));
					bytes = currentBlock.read(buffer, bufferOffset, toRead, offsetInFile);
				} catch (InvalidFileOffsetException e) {
					e.printStackTrace();
					return bufferOffset;
				}
			}
			
			bufferOffset += bytes;
			remaining -= bytes;
			offsetInFile += bytes;
		}
		
		return bufferOffset;
	}
	
	DataBlock getByTime(long timestamp, boolean blockBeforeTime){
		//if timestamp is out of range
		/*if (blocksCount == 0 || timestamp < 0) // || timestamp > lastBlock.lastAppendTime())
			return null;
		
		if (blockBeforeTime && timestamp >= lastBlock.firstAppendTime())
			return lastBlock;
		
		if (!blockBeforeTime && timestamp > lastBlock.lastAppendTime())
			return null;*/
		
		/*
		 * This is a bad approach because we have to traverse the index tree several times. Instead,
		 * do binary search on the direct blocks if the timestamp is within the range of the direct
		 * blocks. Otherwise, recurse into the indirectBlock or doubleIndirectBlock, and do
		 * binary search at the last index level.
		 */
		
		/*DataBlock leftBlock = null;
		DataBlock rightBlock = lastBlock;
		try {
			//Get the first block as the leftBlock
			leftBlock = getByFileOffset(0);
		} catch (InvalidFileOffsetException e1) {
			e1.printStackTrace();
			return null;
		}
		
		//If timestamp is within the first block
		if (timestamp >= leftBlock.creationTime() && timestamp <= leftBlock.lastAppendTime())
			return leftBlock;
		
		if (!blockBeforeTime && timestamp < leftBlock.firstAppendTime())
			return leftBlock;
		
		long midBlockNum = 0;
		while(leftBlock.index() < rightBlock.index()){
			midBlockNum = leftBlock.index() + (rightBlock.index() - leftBlock.index())/2;
			long fileOffset = midBlockNum * Constants.dataBlockSizeBytes;
			DataBlock midBlock = null;
			try {
				midBlock = getByFileOffset(fileOffset);
			} catch (InvalidFileOffsetException e) {
				e.printStackTrace();
				return null;
			}
			
			if (timestamp >= midBlock.creationTime() && timestamp <= midBlock.lastAppendTime())
				return midBlock;
			
			if (leftBlock == midBlock) {
				if (blockBeforeTime)
					return leftBlock;
				else
					return rightBlock;
			}
			
			if (timestamp < midBlock.creationTime()){
				rightBlock = midBlock;
			} else { //otherwise timestamp > block.lastAppendTime()
				leftBlock = midBlock;
			}
		}*/
		
		return null;
	}
	
	/**
	 * Append data at the end of the file
	 * @param data Data to append
	 * @param offset Offset in the data array
	 * @param length Number of bytes to write from the data array
	 * @return number of bytes appended
	 * @throws InvalidFileOffsetException 
	 */
	public int append(byte[] data, int offset, int length) throws MaxFileSizeExceededException, InvalidFileOffsetException{
		int remaining = length;
		int appended = 0;
		long fileSize = this.fileSize;
		
		Cache cache = Cache.instance();
		
		while(remaining > 0){
			int lastBlkCapacity = (int)(fileSize % Constants.dataBlockSizeBytes);
			if (lastBlkCapacity == 0){
				try {
					createNewBlock();
					lastBlkCapacity = Constants.dataBlockSizeBytes;
				} catch (IndexBlockFullException e) {
					e.printStackTrace();
					return appended;
				}
			}

			int bytes;
			int toAppend = remaining <= lastBlkCapacity ? remaining : lastBlkCapacity;
			try (DataBlock block = (DataBlock)cache.acquireBlock(lastBlockUuid)) {
				bytes = block.append(data, offset, toAppend, fileSize);
			}
			
			remaining -= bytes;
			offset += bytes;
			appended += bytes;
			fileSize += bytes;
			//dirty = true;
		}
		
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
	
	DataBlock createNewBlock() throws MaxFileSizeExceededException, IndexBlockFullException, InvalidFileOffsetException{
		//long blocksCount = (long)Math.ceil(1.0*fileSize / Constants.dataBlockSizeBytes);
		if (fileSize + Constants.dataBlockSizeBytes > maxFileSize){
			throw new MaxFileSizeExceededException();
		}
		
		Cache cache = Cache.instance();
		DataBlock dataBlock = cache.newDataBlock();
		dataBlock.blockNumber = blocksCount+1;
		
		long indirectBlocksLimit = Constants.directBlocksPerInode + Commons.maxBlocksCount(1);
		long doubleIndirectBlocksLimit = indirectBlocksLimit + Commons.maxBlocksCount(2);
		long tripleIndirectBlocksLimit = doubleIndirectBlocksLimit + Commons.maxBlocksCount(3);
		
		if (blocksCount < Constants.directBlocksPerInode) {
			//System.out.println("1. Adding direct block "+directBlocksCreated);
			directBlockUuid[(int)blocksCount] = dataBlock.uuid();
		} else {
			IndexBlock indBlock = null;
			
			if (blocksCount < indirectBlocksLimit) {
				//System.out.println("1. Adding indirect block "+blocksCount);
				if (indirectBlockUuid.uuidHigh == 0 && indirectBlockUuid.uuidLow == 0) {
					DataBlock indDataBlock = cache.newDataBlock();
					indBlock = new IndexBlock(indDataBlock.uuid(), 1); //IndexBlock is like a wrapper on DataBlock
					indirectBlockUuid = indBlock.uuid();
					System.out.println("\t\tCreated indirect block: " + Commons.uuidToString(indirectBlockUuid.uuidHigh, indirectBlockUuid.uuidLow));
				} else {
					indBlock = new IndexBlock(indirectBlockUuid, 1);
				}
			} else if (blocksCount < doubleIndirectBlocksLimit){
				if (doubleIndirectBlockUuid.uuidHigh == 0 && doubleIndirectBlockUuid.uuidLow == 0) {
					//System.out.println("1. Adding doubleIndirect block "+blocksCount);
					DataBlock indDataBlock = cache.newDataBlock();
					indBlock = new IndexBlock(indDataBlock.uuid(), 2);
					doubleIndirectBlockUuid = indBlock.uuid();
					System.out.println("\t\tCreated double indirect block.");
				} else {
					indBlock = new IndexBlock(doubleIndirectBlockUuid, 2);
				}
				
			} else if (blocksCount < tripleIndirectBlocksLimit){
				if (tripleIndirectBlockUuid.uuidHigh == 0 && tripleIndirectBlockUuid.uuidLow == 0) {
					//System.out.println("1. Adding tripleIndirect block "+blocksCount);
					DataBlock indDataBlock = cache.newDataBlock();
					indBlock = new IndexBlock(indDataBlock.uuid(), 3);
					tripleIndirectBlockUuid = indBlock.uuid();
					System.out.println("\t\tCreated triple indirect block.");
				} else {
					indBlock = new IndexBlock(tripleIndirectBlockUuid, 3);
				}
			} else {
				throw new MaxFileSizeExceededException();
			}
			
			indBlock.addBlock(dataBlock, blocksCount);
		}
		
		blocksCount++;
		lastBlockUuid = dataBlock.uuid();
		
		//System.out.println("Total file blocks: " + blocksCount);
		
		return dataBlock;
	}
	
	public long fileSize(){
		return fileSize;
	}
	
	/*public long id(){
		return inumber;
	}*/
	
	/*
	 * We need a way to ensure that the concurrent threads do not update this Inode while the block
	 * is being written to the buffer.
	 */
	void toBuffer(ByteBuffer buffer) throws InsufficientResourcesException{
		
		
		int inodeSize = Constants.inodeSizeBytes;
		if (buffer.capacity() < inodeSize)
			throw new InsufficientResourcesException(String.format("Buffer capacity is less than "
					+ "required: Capacity = %d bytes, required = %d bytes.",buffer.capacity(),inodeSize));
		
		int initPosition = buffer.position();
		//buffer.putLong(inumber);
		
		buffer.putLong(fileSize);
		
		for(int i=0; i<directBlockUuid.length; i++){
			buffer.putLong(directBlockUuid[i].uuidHigh);
			buffer.putLong(directBlockUuid[i].uuidLow);
		}
		
		buffer.putLong(indirectBlockUuid.uuidHigh);
		buffer.putLong(indirectBlockUuid.uuidLow);
		
		buffer.putLong(doubleIndirectBlockUuid.uuidHigh);
		buffer.putLong(doubleIndirectBlockUuid.uuidLow);
		
		buffer.putLong(tripleIndirectBlockUuid.uuidHigh);
		buffer.putLong(tripleIndirectBlockUuid.uuidLow);
		
		buffer.putLong(lastBlockUuid.uuidHigh);
		buffer.putLong(lastBlockUuid.uuidLow);
		
		int dataLength = buffer.position() - initPosition;
		int padLength = Math.max(0, Constants.inodeSizeBytes - dataLength);
		
		assert dataLength + padLength == Constants.inodeSizeBytes;
		
		if (padLength > 0) {
			byte[] padding = new byte[padLength];
			buffer.put(padding);
		}
	}
	
	static Inode fromBuffer(ByteBuffer buffer) throws InsufficientResourcesException{
		int inodeSize = Constants.inodeSizeBytes;
		if (buffer.remaining() < inodeSize)
			throw new InsufficientResourcesException(String.format("Buffer has less bytes remaining: "
					+ "%d bytes are remaining, %d bytes are required.",buffer.remaining(),inodeSize));
		
		if (buffer.remaining() < Constants.inodeSizeBytes)
			throw new BufferUnderflowException();
		
		//inumber = buffer.getLong();
		
		int initPosition = buffer.position();
		
		Inode inode = new Inode();
		inode.fileSize = buffer.getLong();
		
		inode.directBlockUuid = new BlockID[Constants.directBlocksPerInode];
		for (int i=0; i<Constants.directBlocksPerInode; i++){
			long uuidHigh = buffer.getLong();
			long uuidLow = buffer.getLong();
			inode.directBlockUuid[i] = new BlockID(uuidHigh, uuidLow, DataBlock.name(uuidHigh, uuidLow), BlockType.DataBlock);
		}
		
		long high = buffer.getLong();
		long low = buffer.getLong();
		inode.indirectBlockUuid = new BlockID(high, low, DataBlock.name(high, low), BlockType.DataBlock);
		
		high = buffer.getLong();
		low = buffer.getLong();
		inode.doubleIndirectBlockUuid = new BlockID(high, low, DataBlock.name(high, low), BlockType.DataBlock);
		
		high = buffer.getLong();
		low = buffer.getLong();
		inode.tripleIndirectBlockUuid = new BlockID(high, low, DataBlock.name(high, low), BlockType.DataBlock);
		
		high = buffer.getLong();
		low = buffer.getLong();
		inode.lastBlockUuid = new BlockID(high, low, DataBlock.name(high, low), BlockType.DataBlock);
		
		int dataLength = buffer.position() - initPosition;
		int padLength = Constants.inodeSizeBytes - dataLength;
		buffer.position(buffer.position()+padLength);
		
		inode.blocksCount = inode.fileSize / Constants.dataBlockSizeBytes;
		
		return inode;
	}
	
	public static int inodesSize(){
		return (Constants.directBlocksPerInode + 4) * 16 + 8 + 32;
	}
	
	boolean dirty(){
		return dirty;
	}
	
	void clear(){
		dirty = false;
	}
}
