package kawkab.fs.core;

import kawkab.fs.commons.Constants;
import kawkab.fs.core.exceptions.IndexBlockFullException;
import kawkab.fs.core.exceptions.InvalidFileOffsetException;
import kawkab.fs.core.exceptions.MaxFileSizeExceededException;
import kawkab.fs.core.exceptions.OutOfMemoryException;

public class FileIndex {
	private final long indexID;
	private String filename;
	private long blocksCount;
	private BlockMetadata lastBlock;
	private BlockMetadata[] directBlocks;
	private int directBlocksCreated;
	private IndexBlock indirectBlock;
	private IndexBlock doubleIndirectBlock;
	private long fileSize = 0;
	
	private static final long directBlocksBytesLimit;
	private static final long indirectBlockBytesLimit;
	private static final long doubleIndirectBlockBytesLimit;
	private static final long maxDataBlocks;
	
	static {
		directBlocksBytesLimit = (long)Constants.maxDirectBlocks*Constants.defaultBlockSize;
		indirectBlockBytesLimit = directBlocksBytesLimit + IndexBlock.maxDataSize(1);
		doubleIndirectBlockBytesLimit = Constants.fileSizeLimit;
		maxDataBlocks = (long)Math.ceil(Constants.fileSizeLimit / Constants.defaultBlockSize);
		
		//To ensure that our index size do not overflow the "long" value, and at has at least one data block.
		assert directBlocksBytesLimit > 0;
		assert directBlocksBytesLimit  < indirectBlockBytesLimit;
		assert indirectBlockBytesLimit >= directBlocksBytesLimit + Constants.defaultBlockSize; 
		assert indirectBlockBytesLimit < doubleIndirectBlockBytesLimit;
		assert doubleIndirectBlockBytesLimit <= indirectBlockBytesLimit+IndexBlock.maxDataSize(2);
	}
	
	public FileIndex(long indexID, String filename){
		this.indexID = indexID;
		this.filename = filename;
		directBlocks = new BlockMetadata[Constants.maxDirectBlocks];
		directBlocksCreated = 0;
	}
	
	protected synchronized BlockMetadata appendNew() throws OutOfMemoryException, MaxFileSizeExceededException, IndexBlockFullException{
		if (blocksCount >= maxDataBlocks){
			throw new MaxFileSizeExceededException();
		}
		
		long fileSize = blocksCount*Constants.defaultBlockSize;
		BlockMetadata block = new BlockMetadata(blocksCount, fileSize);
		
		long newFileSize = (blocksCount + 1) * Constants.defaultBlockSize;
		
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
		} else {
			throw new MaxFileSizeExceededException();
		}
		
		blocksCount++;
		lastBlock = block;
		
		System.out.println("Total file blocks: " + blocksCount);
		
		return block;
	}
	
	public BlockMetadata getByByte(long byteOffset) throws InvalidFileOffsetException{
		if (byteOffset > Constants.fileSizeLimit){
			throw new InvalidFileOffsetException();
		}
		
		BlockMetadata block = null;
		if (byteOffset <= directBlocksBytesLimit){
			int blockIdx = (int)(byteOffset/Constants.defaultBlockSize);
			block = directBlocks[blockIdx];
		} else if (byteOffset <= indirectBlockBytesLimit){
			block = indirectBlock.getByByte(byteOffset);
		}  else {
			block = doubleIndirectBlock.getByByte(byteOffset);
		}
		
		return block;
	}
	
	public BlockMetadata getByTime(long timestamp){
		return null;
	}
	
	protected synchronized BlockMetadata getLastBlock(){
		return lastBlock;
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
			BlockMetadata block = getLastBlock();
			
			if (block == null || block.capacity() == 0){
				try {
					block = appendNew();
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
			
			if (block.capacity() == 0){
				block.close();
			}
		}
		
		return appended;
	}
}
