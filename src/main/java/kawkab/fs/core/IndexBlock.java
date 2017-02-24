package kawkab.fs.core;

import kawkab.fs.commons.Constants;
import kawkab.fs.core.exceptions.IndexBlockFullException;

public class IndexBlock {
	public static final int maxNumPointers = Constants.numPointersInIndexBlock;
	
	private int indexLevel;
	private BlockMetadata firstBlock;
	private BlockMetadata lastBlock;
	
	private final BlockMetadata[] dataBlocks;
	private final IndexBlock[] indexBlocks;
	private int appendAt;
	private long blocksCount = 0;
	
	//private final long maxDataSize;
	private final long maxDataPerPointer;
	private final long maxDataBlocks;
	
	public IndexBlock(int indexLevel){
		assert indexLevel > 0;
		assert indexLevel <= Constants.maxIndexLevels; //To ensure that the size of data is less than 2^64-1 
		
		this.indexLevel = indexLevel;
		
		if (indexLevel == 1) {
			dataBlocks  = new BlockMetadata[maxNumPointers];
			indexBlocks = null;
			
			maxDataPerPointer = Constants.defaultBlockSize;
			//maxDataSize = maxNumPointers * Constants.defaultBlockSize;
		} else {
			dataBlocks = null;
			indexBlocks = new IndexBlock[maxNumPointers];
			
			maxDataPerPointer = (long)Math.pow(maxNumPointers, indexLevel-1)*Constants.defaultBlockSize;
			//maxDataSize = maxDataBlocks * Constants.defaultBlockSize;
		}
		
		maxDataBlocks = (long)Math.pow(maxNumPointers, indexLevel);
	}
	
	public synchronized void addBlock(BlockMetadata block) throws IndexBlockFullException{
		if (!canAddBlock()){
			throw new IndexBlockFullException("Cannot add a new block.");
		}
		
		if (indexLevel == 1) {
			appendDataBlock(block);
			return;
		}
		
		if (blocksCount == 0){
			indexBlocks[appendAt] = new IndexBlock(indexLevel-1);
		}
		
		if (indexBlocks[appendAt].canAddBlock()){
			indexBlocks[appendAt].addBlock(block);
		} else {
			appendAt++;
			indexBlocks[appendAt] = new IndexBlock(indexLevel-1);
			indexBlocks[appendAt].addBlock(block);
			if (blocksCount == 0){
				firstBlock = block;
			}
		}
		
		blocksCount++;
		lastBlock = block;
	}
	
	private synchronized void appendDataBlock(BlockMetadata block) throws IndexBlockFullException{
		assert indexLevel == 1;
		
		dataBlocks[appendAt] = block;
		appendAt++;
		
		if (appendAt == 1){
			firstBlock = block;
		}
		
		blocksCount++;
		lastBlock = block;
	}
	
	public BlockMetadata getByByte(long byteOffset){
		long localOffset = byteOffset - firstBlock.offset();
		int idx = (int)(localOffset/maxDataPerPointer);
		
		if (indexLevel == 1){
			return dataBlocks[idx];
		}
		
		return indexBlocks[idx].getByByte(byteOffset);
	}
	
	public BlockMetadata getByTime(long timestamp){
		return null;
	}
	
	public synchronized boolean canAddBlock(){
		return blocksCount < maxDataBlocks;
	}
	
	public long firstByteOffset(){
		if (blocksCount == 0)
			return -1;
		
		return firstBlock.offset();
	}
	
	public long lastByteOffset(){
		if (blocksCount == 0)
			return -1;
		
		return lastBlock.offset()+lastBlock.size();
	}
	
	public long firstBlockTime(){
		if (blocksCount == 0)
			return -1;
		
		return firstBlock.creationTime();
	}
	
	public long lastBlockTime(){
		if (blocksCount == 0)
			return -1;
		
		return lastBlock.creationTime();
	}
	
	public static long maxDataSize(int indexLevel){
		return (long)Math.pow(maxNumPointers, indexLevel) * Constants.defaultBlockSize;
	}
	
}
