package kawkab.fs.core;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

import javax.naming.InsufficientResourcesException;

import kawkab.fs.commons.Commons;
import kawkab.fs.commons.Constants;
import kawkab.fs.core.exceptions.IndexBlockFullException;
import kawkab.fs.core.exceptions.InvalidArgumentsException;
import kawkab.fs.core.exceptions.InvalidFileOffsetException;
import kawkab.fs.core.exceptions.MaxFileSizeExceededException;
import kawkab.fs.core.exceptions.OutOfMemoryException;

public class Inode {
	//private long inumber;
	private long blocksCount;
	private long fileSize;
	private int directBlocksCreated;
	private long directBlockUuidLow[];
	private long directBlockUuidHigh[];
	private long indirectBlockUuidLow;
	private long indirectBlockUuidHigh;
	private long doubleIndirectBlockUuidLow;
	private long doubleIndirectBlockUuidHigh;
	private long tripleIndirectBlockUuidLow;
	private long tripleIndirectBlockUuidHigh;
	private long lastBlockUuidHigh;
	private long lastBlockUuidLow;
	private boolean dirty;
	
	/*public INode(){ //long inumber){
		//this.inumber = inumber;
		//directBlocks = new DataBlock[Constants.maxDirectBlocks];
		directBlockUuidLow = new long[Constants.maxDirectBlocks];
		directBlockUuidHigh = new long[Constants.maxDirectBlocks];
		directBlocksCreated = 0;
	}*/
	
	private Inode(){}
	
	static Inode bootstrap(){
		Inode inode = new Inode();
		inode.directBlockUuidLow = new long[Constants.maxDirectBlocks];
		inode.directBlockUuidHigh = new long[Constants.maxDirectBlocks];
		inode.directBlocksCreated = 0;
		return inode;
	}
	
	long[] getByFileOffset(long offsetInFile) throws InvalidFileOffsetException{
		//FIXME: Do we want to throw an exception or return null if there is no data in the file yet?
		if (offsetInFile < 0 || offsetInFile > fileSize){
			throw new InvalidFileOffsetException(String.format("Invalid file offset %d. File size is %d bytes.",offsetInFile,fileSize));
		}
		
		long blockIdx = offsetInFile / Constants.dataBlockSizeBytes;
		long indirectBlocksLimit = Constants.maxDirectBlocks + Commons.maxBlocksCount(1);
		long doubleIndirectBlocksLimit = indirectBlocksLimit + Commons.maxBlocksCount(2);
		
		//System.out.println(String.format("Block=%d, offset=%d", blockIdx, 
		//		offsetInFile, indirectBlocksLimit, doubleIndirectBlocksLimit));
		
		if (blockIdx < Constants.maxDirectBlocks){
			long uuidHigh = directBlockUuidHigh[(int)blockIdx];
			long uuidLow = directBlockUuidLow[(int)blockIdx];
			return new long[]{uuidHigh, uuidLow};
		} else if (blockIdx < indirectBlocksLimit){
			IndexBlock indirectBlock = new IndexBlock(indirectBlockUuidHigh, indirectBlockUuidLow, 1);
			return indirectBlock.getBlockIDByByte(offsetInFile);
		} else if (blockIdx < doubleIndirectBlocksLimit){
			IndexBlock doubleIndirectBlock = new IndexBlock(doubleIndirectBlockUuidHigh, doubleIndirectBlockUuidLow, 2);
			return doubleIndirectBlock.getBlockIDByByte(offsetInFile);
		}
		
		IndexBlock tripleIndirectBlock = new IndexBlock(tripleIndirectBlockUuidHigh, tripleIndirectBlockUuidLow, 3);
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
			long[] uuids = getByFileOffset(offsetInFile);
			long curBlkUuidHigh = uuids[0];
			long curBlkUuidLow  = uuids[1];
			
			long blockNumber = offsetInFile/Constants.dataBlockSizeBytes;
			long nextBlockStart = (blockNumber + 1) * Constants.dataBlockSizeBytes;
			int toRead = (int)(offsetInFile+remaining <= nextBlockStart ? remaining : nextBlockStart - offsetInFile);
			int bytes = 0;
			try {
				//System.out.println("  Reading block " + Commons.uuidToString(curBlkUuidHigh, curBlkUuidLow));
				DataBlock currentBlock = cache.getDataBlock(curBlkUuidHigh, curBlkUuidLow);
				bytes = currentBlock.read(buffer, bufferOffset, toRead, offsetInFile);
			} catch (InvalidFileOffsetException e) {
				e.printStackTrace();
				return bufferOffset;
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
	 * @return Index of the data appended to the file. The caller can use 
	 * dataIndex.timestamp() to refer to the data just written. 
	 * @throws OutOfMemoryException 
	 * @throws InvalidFileOffsetException 
	 */
	synchronized int append(byte[] data, int offset, int length) throws OutOfMemoryException, 
					MaxFileSizeExceededException, InvalidFileOffsetException{
		int remaining = length;
		int appended = 0;
		
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
			
			int toAppend = remaining <= lastBlkCapacity ? remaining : lastBlkCapacity;
			DataBlock block = cache.getDataBlock(lastBlockUuidHigh, lastBlockUuidLow);
			int bytes = block.append(data, offset, toAppend, fileSize);
			remaining -= bytes;
			offset += bytes;
			appended += bytes;
			fileSize += bytes;
		}
		
		dirty = true;
		return appended;
	}
	
	synchronized DataBlock createNewBlock() throws OutOfMemoryException, 
					MaxFileSizeExceededException, IndexBlockFullException, InvalidFileOffsetException{
		if (blocksCount >= (long)Math.ceil(Constants.fileSizeLimit / Constants.dataBlockSizeBytes)){
			throw new MaxFileSizeExceededException();
		}
		
		Cache cache = Cache.instance();
		DataBlock dataBlock = cache.newDataBlock();
		
		long indirectBlocksLimit = Constants.maxDirectBlocks + Commons.maxBlocksCount(1);
		long doubleIndirectBlocksLimit = indirectBlocksLimit + Commons.maxBlocksCount(2);
		long tripleIndirectBlocksLimit = doubleIndirectBlocksLimit + Commons.maxBlocksCount(3);
		
		if (blocksCount < Constants.maxDirectBlocks) {
			//System.out.println("1. Adding direct block "+directBlocksCreated);
			assert blocksCount == directBlocksCreated;
			directBlockUuidHigh[directBlocksCreated] = dataBlock.uuidHigh();
			directBlockUuidLow[directBlocksCreated] = dataBlock.uuidLow();
			directBlocksCreated++;
		} else {
			assert directBlocksCreated == directBlockUuidLow.length;
			IndexBlock indBlock = null;
			
			if (blocksCount < indirectBlocksLimit) {
				//System.out.println("1. Adding indirect block "+blocksCount);
				if (indirectBlockUuidHigh == 0 && indirectBlockUuidLow == 0) {
					DataBlock indDataBlock = cache.newDataBlock();
					indBlock = new IndexBlock(indDataBlock.uuidHigh(), indDataBlock.uuidLow(), 1);
					indirectBlockUuidHigh = indBlock.uuidHigh();
					indirectBlockUuidLow = indBlock.uuidLow();
					System.out.println("\t\tCreated indirect block: " + Commons.uuidToString(indirectBlockUuidHigh, indirectBlockUuidLow));
				} else {
					indBlock = new IndexBlock(indirectBlockUuidHigh, indirectBlockUuidLow, 1);
				}
			} else if (blocksCount < doubleIndirectBlocksLimit){
				if (doubleIndirectBlockUuidHigh == 0 && doubleIndirectBlockUuidLow == 0) {
					//System.out.println("1. Adding doubleIndirect block "+blocksCount);
					DataBlock indDataBlock = cache.newDataBlock();
					indBlock = new IndexBlock(indDataBlock.uuidHigh(), indDataBlock.uuidLow(), 2);
					doubleIndirectBlockUuidHigh = indBlock.uuidHigh();
					doubleIndirectBlockUuidLow = indBlock.uuidLow();
					System.out.println("\t\tCreated double indirect block.");
				} else {
					indBlock = new IndexBlock(doubleIndirectBlockUuidHigh, doubleIndirectBlockUuidLow, 2);
				}
				
			} else if (blocksCount < tripleIndirectBlocksLimit){
				if (tripleIndirectBlockUuidHigh == 0 && tripleIndirectBlockUuidLow == 0) {
					//System.out.println("1. Adding tripleIndirect block "+blocksCount);
					DataBlock indDataBlock = cache.newDataBlock();
					indBlock = new IndexBlock(indDataBlock.uuidHigh(), indDataBlock.uuidLow(), 3);
					tripleIndirectBlockUuidHigh = indBlock.uuidHigh();
					tripleIndirectBlockUuidLow = indBlock.uuidLow();
					System.out.println("\t\tCreated triple indirect block.");
				} else {
					indBlock = new IndexBlock(tripleIndirectBlockUuidHigh, tripleIndirectBlockUuidLow, 3);
				}
			} else {
				throw new MaxFileSizeExceededException();
			}
			
			indBlock.addBlock(dataBlock, blocksCount);
		}
		
		blocksCount++;
		fileSize += Constants.dataBlockSizeBytes;;
		dirty = true;
		
		lastBlockUuidHigh = dataBlock.uuidHigh();
		lastBlockUuidLow = dataBlock.uuidLow();
		
		System.out.println("Total file blocks: " + blocksCount);
		
		return dataBlock;
	}
	
	long fileSize(){
		return fileSize;
	}
	
	long blocksCount(){
		return blocksCount;
	}
	
	/*public long id(){
		return inumber;
	}*/
	
	void toBuffer(ByteBuffer buffer) throws InsufficientResourcesException{
		int inodeSize = Constants.inodeSizeBytes;
		if (buffer.capacity() < inodeSize)
			throw new InsufficientResourcesException(String.format("Buffer capacity is less than "
					+ "required: Capacity = %d bytes, required = %d bytes.",buffer.capacity(),inodeSize));
		
		int initPosition = buffer.position();
		//buffer.putLong(inumber);
		
		buffer.putLong(blocksCount);
		buffer.putLong(fileSize);
		buffer.putInt(directBlocksCreated);
		
		for(int i=0; i<directBlockUuidLow.length; i++){
			buffer.putLong(directBlockUuidLow[i]);
			buffer.putLong(directBlockUuidHigh[i]);
		}
		
		buffer.putLong(indirectBlockUuidLow);
		buffer.putLong(indirectBlockUuidHigh);
		
		buffer.putLong(doubleIndirectBlockUuidLow);
		buffer.putLong(doubleIndirectBlockUuidHigh);
		
		buffer.putLong(tripleIndirectBlockUuidLow);
		buffer.putLong(tripleIndirectBlockUuidHigh);
		
		buffer.putLong(lastBlockUuidHigh);
		buffer.putLong(lastBlockUuidLow);
		
		int dataLength = buffer.position() - initPosition;
		int padLength = Constants.inodeSizeBytes - dataLength;
		
		byte[] padding = new byte[padLength];
		buffer.put(padding);
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
		inode.blocksCount = buffer.getLong();
		inode.fileSize = buffer.getLong();
		inode.directBlocksCreated = buffer.getInt();
		
		inode.directBlockUuidLow = new long[Constants.maxDirectBlocks];
		inode.directBlockUuidHigh = new long[Constants.maxDirectBlocks];
		for (int i=0; i<Constants.maxDirectBlocks; i++){
			inode.directBlockUuidLow[i] = buffer.getLong();
			inode.directBlockUuidHigh[i] = buffer.getLong();
		}
		
		inode.indirectBlockUuidLow = buffer.getLong();
		inode.indirectBlockUuidHigh = buffer.getLong();
		
		inode.doubleIndirectBlockUuidLow = buffer.getLong();
		inode.doubleIndirectBlockUuidHigh = buffer.getLong();
		
		inode.tripleIndirectBlockUuidLow = buffer.getLong();
		inode.tripleIndirectBlockUuidHigh = buffer.getLong();
		
		inode.lastBlockUuidHigh = buffer.getLong();
		inode.lastBlockUuidLow = buffer.getLong();
		
		int dataLength = buffer.position() - initPosition;
		int padLength = Constants.inodeSizeBytes - dataLength;
		buffer.position(buffer.position()+padLength);
		
		return inode;
	}
	
	boolean dirty(){
		return dirty;
	}
	
	void clear(){
		dirty = false;
	}
}
