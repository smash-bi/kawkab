package kawkab.fs.core;

import java.nio.ByteBuffer;

import kawkab.fs.commons.Constants;
import kawkab.fs.core.exceptions.IndexBlockFullException;
import kawkab.fs.persistence.Cache;

public class IndexBlock {
	public static final int maxNumPointers = Constants.numPointersInIndexBlock;
	
	//private long uuidHigh;
	//private long uuidLow;
	private int indexLevel;
	private int pointersAdded;
	private long blocksCount = 0; //Total blocks added under this index.
	private static int headerSize = 4+4+8; //indexLevel, pointersAdded, blocksCount.
	
	//private BlockMetadata firstBlock;
	//private BlockMetadata lastBlock;
	
	//private final BlockMetadata[] dataBlocks;
	//private final IndexBlock[] indexBlocks;
	//private int appendAt;
	
	//private final long maxDataSize;
	//private final long maxDataPerPointer;
	//private final long maxDataBlocks;
	
	private Cache cache;
	
	private IndexBlock(){}
	
	public IndexBlock(int indexLevel){
		assert indexLevel > 0;
		assert indexLevel <= Constants.maxIndexLevels; //To ensure that the size of data is less than 2^64-1 
		
		this.indexLevel = indexLevel;
		
		cache = Cache.instance();
		BlockMetadata block = cache.newDataBlock();
		block.fill();
		
		this.uuidHigh = block.uuidHigh();
		this.uuidLow = block.uuidLow();
		
		/*if (indexLevel == 1) {
			dataBlocks  = new BlockMetadata[maxNumPointers];
			indexBlocks = null;
			
			maxDataPerPointer = Constants.dataBlockSizeBytes;
			//maxDataSize = maxNumPointers * Constants.defaultBlockSize;
		} else {
			dataBlocks = null;
			indexBlocks = new IndexBlock[maxNumPointers];
			
			maxDataPerPointer = (long)Math.pow(maxNumPointers, indexLevel-1)*Constants.dataBlockSizeBytes;
			//maxDataSize = maxDataBlocks * Constants.defaultBlockSize;
		}*/
		
		//maxDataBlocks = (long)Math.pow(maxNumPointers, indexLevel);
	}
	
	public static IndexBlock fromDataBlock(long dataBlockUuidHigh, long dataBlockUuidLow, int indexLevel){
		assert indexLevel > 0;
		assert indexLevel <= Constants.maxIndexLevels;
		
		IndexBlock block = new IndexBlock();
		
		block.indexLevel = indexLevel;
		block.uuidHigh = dataBlockUuidHigh;
		block.uuidLow = dataBlockUuidLow;
		block.cache = Cache.instance();
		BlockMetadata dataBlock = block.cache.getDataBlock(dataBlockUuidHigh, dataBlockUuidLow);
		ByteBuffer buffer = ByteBuffer.wrap(dataBlock.data());
		
		block.
		
		return block;
	}
	
	public synchronized void addBlock(BlockMetadata block) throws IndexBlockFullException{
		if (!canAddBlock()){
			throw new IndexBlockFullException("Cannot add a new block.");
		}
		
		if (indexLevel == 1) {
			appendDataBlock(block);
			return;
		}
		
		BlockMetadata indBlock = cache.getDataBlock(uuidHigh, uuidLow);
		ByteBuffer data = ByteBuffer.wrap(indBlock.data());
		data.position(headerSize);
		
		
		
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
		return (long)Math.pow(maxNumPointers, indexLevel) * Constants.dataBlockSizeBytes;
	}
	
	public static long maxBlocksCount(int indexLevel){
		return (long)Math.pow(maxNumPointers, indexLevel);
	}
	
	public long uuidHigh(){
		return uuidHigh;
	}
	
	public long uuidLow(){
		return uuidLow;
	}
	
}
