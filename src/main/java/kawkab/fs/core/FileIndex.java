package kawkab.fs.core;

import kawkab.fs.commons.Constants;
import kawkab.fs.core.exceptions.IndexBlockFullException;
import kawkab.fs.core.exceptions.InvalidFileOffsetException;
import kawkab.fs.core.exceptions.MaxFileSizeExceededException;
import kawkab.fs.core.exceptions.OutOfMemoryException;

public class FileIndex {
	private final long id;
	private long blocksCount;
	private long fileSize = 0;
	private BlockMetadata lastBlock;
	private BlockMetadata[] directBlocks;
	private int directBlocksCreated;
	private IndexBlock indirectBlock;
	private IndexBlock doubleIndirectBlock;
	private IndexBlock tripleIndirectBlock;
	
	//private static final long blockSize;
	private static final long directBlocksBytesLimit;
	private static final long indirectBlockBytesLimit;
	private static final long doubleIndirectBlockBytesLimit;
	private static final long tripleIndirectBlockBytesLimit;
	private static final long maxDataBlocks;
	
	static {
		//blockSize = Constants.dataBlockSizeBytes;
		directBlocksBytesLimit = (long)Constants.maxDirectBlocks*Constants.dataBlockSizeBytes;
		indirectBlockBytesLimit = directBlocksBytesLimit + IndexBlock.maxDataSize(1);
		doubleIndirectBlockBytesLimit = indirectBlockBytesLimit+IndexBlock.maxDataSize(2); //FIXME: What is its impact on the maximum file size limit?
		tripleIndirectBlockBytesLimit = doubleIndirectBlockBytesLimit + IndexBlock.maxDataSize(3);
		maxDataBlocks = (long)Math.ceil(Constants.fileSizeLimit / Constants.dataBlockSizeBytes);
		
		//To ensure that our index size do not overflow the "long" value, and at has at least one data block.
		assert directBlocksBytesLimit > 0;
		assert directBlocksBytesLimit  < indirectBlockBytesLimit;
		assert indirectBlockBytesLimit >= directBlocksBytesLimit + Constants.dataBlockSizeBytes; 
		assert indirectBlockBytesLimit < doubleIndirectBlockBytesLimit;
	}
	
	public FileIndex(long indexID){
		this.id = indexID;
		directBlocks = new BlockMetadata[Constants.maxDirectBlocks];
		directBlocksCreated = 0;
	}
	
	protected synchronized BlockMetadata createNewBlock() throws OutOfMemoryException, MaxFileSizeExceededException, IndexBlockFullException{
		if (blocksCount >= maxDataBlocks){
			throw new MaxFileSizeExceededException();
		}
		
		long fileSize = blocksCount*Constants.dataBlockSizeBytes;
		BlockMetadata block = new BlockMetadata(blocksCount, fileSize);
		
		long newFileSize = (blocksCount + 1) * Constants.dataBlockSizeBytes;
		
		if (newFileSize <= directBlocksBytesLimit) {
			assert directBlocksCreated < directBlocks.length;
			directBlocks[directBlocksCreated] = block;
			directBlocksCreated++;
		} else if (newFileSize <= indirectBlockBytesLimit){
			if (indirectBlock == null) {
				indirectBlock = new IndexBlock(1);
				System.out.println("Created indirect block.");
			}
			
			indirectBlock.addBlock(block);
		} else if (newFileSize <= doubleIndirectBlockBytesLimit){
			if (doubleIndirectBlock == null){
				doubleIndirectBlock = new IndexBlock(2);
				System.out.println("Created double indirect block.");
			}
			
			doubleIndirectBlock.addBlock(block);
		} else if (newFileSize <= tripleIndirectBlockBytesLimit){
			if (tripleIndirectBlock == null){
				tripleIndirectBlock = new IndexBlock(3);
				System.out.println("Created triple indirect block.");
			}
			
			tripleIndirectBlock.addBlock(block);
		} else {
			throw new MaxFileSizeExceededException();
		}
		
		blocksCount++;
		lastBlock = block;
		
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
}
