package kawkab.fs.core;

import java.nio.ByteBuffer;

import kawkab.fs.commons.Constants;
import kawkab.fs.core.exceptions.IndexBlockFullException;
import kawkab.fs.core.exceptions.InvalidFileOffsetException;
import kawkab.fs.core.exceptions.MaxFileSizeExceededException;
import kawkab.fs.core.exceptions.OutOfMemoryException;
import kawkab.fs.persistence.Cache;

public class FileIndex {
	private long id;
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
	private boolean dirty;
	
	private Cache cache;
	
	public FileIndex(long indexID){
		this.id = indexID;
		//directBlocks = new BlockMetadata[Constants.maxDirectBlocks];
		directBlockUuidLow = new long[Constants.maxDirectBlocks];
		directBlockUuidHigh = new long[Constants.maxDirectBlocks];
		directBlocksCreated = 0;
		
		cache = Cache.instance();
	}
	
	private synchronized BlockMetadata createNewBlock() throws OutOfMemoryException, MaxFileSizeExceededException, IndexBlockFullException{
		if (blocksCount >= (long)Math.ceil(Constants.fileSizeLimit / Constants.dataBlockSizeBytes)){
			throw new MaxFileSizeExceededException();
		}
		
		long fileSize = blocksCount*Constants.dataBlockSizeBytes;
		BlockMetadata block = cache.newDataBlock();
		
		long newFileSize = (blocksCount + 1) * Constants.dataBlockSizeBytes;
		long indirectBlocksLimit = Constants.maxDirectBlocks + IndexBlock.maxBlocksCount(1);
		long doubleIndirectBlocksLimit = indirectBlocksLimit + IndexBlock.maxBlocksCount(2);
		long tripleIndirectBlocksLimit = doubleIndirectBlocksLimit + IndexBlock.maxBlocksCount(3);
		
		if (blocksCount < Constants.maxDirectBlocks) {
			assert blocksCount == directBlocksCreated;
			directBlockUuidHigh[directBlocksCreated] = block.uuidHigh();
			directBlockUuidLow[directBlocksCreated] = block.uuidLow();
			directBlocksCreated++;
		} else if (blocksCount < indirectBlocksLimit) {
			assert directBlocksCreated == directBlockUuidLow.length;
			if (indirectBlockUuidHigh == 0 && indirectBlockUuidLow == 0) {
				IndexBlock indBlock = new IndexBlock(1);
				
				indirectBlockUuidHigh = indBlock.uuidHigh();
				indirectBlockUuidLow = indBlock.uuidLow();
				
				System.out.println("Created indirect block.");
			}
			
			IndexBlock indBlock = IndexBlock.fromDataBlock(indirectBlockUuidHigh, indirectBlockUuidLow, 1);
			indBlock.addBlock(block);
		} else if (blocksCount < doubleIndirectBlocksLimit){
			if (doubleIndirectBlock == null){
				doubleIndirectBlock = new IndexBlock(2);
				System.out.println("Created double indirect block.");
			}
			
			doubleIndirectBlock.addBlock(block);
		} else if (blocksCount < tripleIndirectBlocksLimit){
			if (tripleIndirectBlock == null){
				tripleIndirectBlock = new IndexBlock(3);
				System.out.println("Created triple indirect block.");
			}
			
			tripleIndirectBlock.addBlock(block);
		} else {
			throw new MaxFileSizeExceededException();
		}
		
		blocksCount++;
		fileSize = newFileSize;
		
		System.out.println("Total file blocks: " + blocksCount);
		
		return block;
	}
	
	public BlockMetadata getByFileOffset(long byteOffset) throws InvalidFileOffsetException{
		//FIXME: Do we want to throw an exception or return null if there is no data in the file yet?
		if (byteOffset < 0 || byteOffset > fileSize){
			throw new InvalidFileOffsetException(String.format("Invalid file offset: %d. File size is %d.",byteOffset,fileSize));
		}
		
		BlockMetadata block = null;
		if (byteOffset <= directBlocksBytesLimit){
			int blockIdx = (int)(byteOffset/Constants.dataBlockSizeBytes);
			block = directBlocks[blockIdx];
		} else if (byteOffset <= indirectBlockBytesLimit){
			block = indirectBlock.getByByte(byteOffset);
		} else if (byteOffset <= doubleIndirectBlockBytesLimit){
			block = doubleIndirectBlock.getByByte(byteOffset);
		}  else {
			block = tripleIndirectBlock.getByByte(byteOffset);
		}
		
		return block;
	}
	
	public BlockMetadata getByTime(long timestamp, boolean blockBeforeTime){
		//if timestamp is out of range
		if (blocksCount == 0 || timestamp < 0) // || timestamp > lastBlock.lastAppendTime())
			return null;
		
		if (blockBeforeTime && timestamp >= lastBlock.firstAppendTime())
			return lastBlock;
		
		if (!blockBeforeTime && timestamp > lastBlock.lastAppendTime())
			return null;
		
		/*
		 * This is a bad approach because we have to traverse the index tree several times. Instead,
		 * do binary search on the direct blocks if the timestamp is within the range of the direct
		 * blocks. Otherwise, recurse into the indirectBlock or doubleIndirectBlock, and do
		 * binary search at the last index level.
		 */
		
		BlockMetadata leftBlock = null;
		BlockMetadata rightBlock = lastBlock;
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
			BlockMetadata midBlock = null;
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
		}
		
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
	 */
	public synchronized int append(byte[] data, int offset, int length) throws OutOfMemoryException, MaxFileSizeExceededException{
		int remaining = length;
		int appended = 0;
		
		while(remaining > 0){
			BlockMetadata block = lastBlock;
			
			if (block == null || block.capacity() == 0){
				try {
					block = createNewBlock();
				} catch (IndexBlockFullException e) {
					e.printStackTrace();
					return appended;
				}
			}
			
			int capacity = block.capacity();
			int toAppend = remaining <= capacity ? remaining : capacity;
			int bytes = block.append(data, offset, toAppend);
			
			remaining -= bytes;
			offset += bytes;
			appended += bytes;
			fileSize += bytes;
		}
		
		return appended;
	}
	
	public long fileSize(){
		return fileSize;
	}
	
	public long blocksCount(){
		return blocksCount;
	}
	
	public long id(){
		return id;
	}
	
	public int toBuffer(ByteBuffer buffer){
		int initPosition = buffer.position();
		buffer.putLong(id);
		
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
		
		int dataLength = buffer.position() - initPosition;
		int padLength = Constants.inodeSizeBytes - dataLength;
		
		byte[] padding = new byte[padLength];
		buffer.put(padding);
		
		return dataLength + padLength;
	}
	
	public int fromBuffer(ByteBuffer buffer){
		if (buffer.remaining() < Constants.inodeSizeBytes)
			return 0;
		
		int initPosition = buffer.position();
		
		id = buffer.getLong();
		
		blocksCount = buffer.getLong();
		fileSize = buffer.getLong();
		directBlocksCreated = buffer.getInt();
		
		directBlockUuidLow = new long[Constants.maxDirectBlocks];
		directBlockUuidHigh = new long[Constants.maxDirectBlocks];
		for (int i=0; i<Constants.maxDirectBlocks; i++){
			directBlockUuidLow[i] = buffer.getLong();
			directBlockUuidHigh[i] = buffer.getLong();
		}
		
		indirectBlockUuidLow = buffer.getLong();
		indirectBlockUuidHigh = buffer.getLong();
		
		doubleIndirectBlockUuidLow = buffer.getLong();
		doubleIndirectBlockUuidHigh = buffer.getLong();
		
		tripleIndirectBlockUuidLow = buffer.getLong();
		tripleIndirectBlockUuidHigh = buffer.getLong();
		
		int dataLength = buffer.position() - initPosition;
		int padLength = Constants.inodeSizeBytes - dataLength;
		
		byte[] padding = new byte[padLength];
		buffer.get(padding);
		
		return dataLength + padLength;
	}
	
	public boolean dirty(){
		return dirty;
	}
	
	public void clear(){
		dirty = false;
	}
}
