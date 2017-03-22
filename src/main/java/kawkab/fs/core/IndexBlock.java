package kawkab.fs.core;

import kawkab.fs.commons.Commons;
import kawkab.fs.commons.Constants;
import kawkab.fs.core.exceptions.IndexBlockFullException;
import kawkab.fs.core.exceptions.InvalidFileOffsetException;

public class IndexBlock {
	public static final int pointerSizeBytes = 16; //Two long values: uuidHigh and uuidLow
	
	//private long uuidHigh;
	//private long uuidLow;
	private final int indexLevel;
	private final long uuidHigh; //UUID of the dataBlock of this index
	private final long uuidLow;
	//private int pointersAdded;
	//private long blocksCount = 0; //Total blocks added under this index.
	//private static int headerSize = 4+4+8; //indexLevel, pointersAdded, blocksCount.
	
	//private DataBlock firstBlock;
	//private DataBlock lastBlock;
	
	//private final DataBlock[] dataBlocks;
	//private final IndexBlock[] indexBlocks;
	//private int appendAt;
	
	//private final long maxDataSize;
	//private final long maxDataPerPointer;
	//private final long maxDataBlocks;
	
	//private Cache cache;
	
	IndexBlock(long dataBlockUuidHigh, long dataBlockUuidLow, int indexLevel){
		assert indexLevel > 0;
		
		this.uuidHigh = dataBlockUuidHigh;
		this.uuidLow = dataBlockUuidLow;
		this.indexLevel = indexLevel;
	}
	
	/**
	 * @param dataBlock DataBlock to be added in the index
	 * @param blockNumber zero based block number of the new block
	 * @throws IndexBlockFullException
	 * @throws InvalidFileOffsetException 
	 */
	synchronized void addBlock(DataBlock dataBlock, long blockNumber) throws IndexBlockFullException, InvalidFileOffsetException{
		/*if (!canAddBlock()){
			throw new IndexBlockFullException("Cannot add a new block.");
		}*/
		
		if (indexLevel == 1) {
			appendDataBlock(dataBlock, blockNumber);
			return;
		}
		
		//Convert block number according to this index level.
		long blockInThisIndex = adjustBlockNumber(blockNumber) % Commons.maxBlocksCount(indexLevel);
		long blocksPerPointer = Commons.maxBlocksCount(indexLevel-1);
		long indexPointer = blockInThisIndex / blocksPerPointer;
		
		Cache cache = Cache.instance();
		DataBlock thisIndexBlock = cache.getDataBlock(uuidHigh, uuidLow);
		
		int offsetInBlock = (int)indexPointer * pointerSizeBytes;
		long pointerUuidHigh = thisIndexBlock.readLong(offsetInBlock);
		long pointerUuidLow = thisIndexBlock.readLong(offsetInBlock+8);
		
		IndexBlock nextIndexBlock = null;
		if (pointerUuidHigh == 0 && pointerUuidLow == 0){
			DataBlock indexBlock = cache.newDataBlock();
			nextIndexBlock = new IndexBlock(indexBlock.uuidHigh(), indexBlock.uuidLow(), indexLevel-1);
			thisIndexBlock.appendLong(nextIndexBlock.uuidHigh, offsetInBlock);
			thisIndexBlock.appendLong(nextIndexBlock.uuidLow, offsetInBlock+8);
		}else{
			nextIndexBlock = new IndexBlock(pointerUuidHigh, pointerUuidLow, indexLevel-1);
		}
		
		//System.out.println(String.format("\t\t\t %d => Adding block %d->%d, indexPointer %d, next uuid %s", 
		//		indexLevel, blockNumber, blockInThisIndex, indexPointer, Commons.uuidToString(nextIndexBlock.uuidHigh, nextIndexBlock.uuidLow)));
		
		nextIndexBlock.addBlock(dataBlock, blockNumber);
	}
	
	synchronized void appendDataBlock(DataBlock dataBlock, long globalBlockNumber) throws IndexBlockFullException{
		assert indexLevel == 1;
		
		int blockNumber = (int)(adjustBlockNumber(globalBlockNumber) % Commons.maxBlocksCount(1));
		int offsetInIdxBlock = (int)(blockNumber) * pointerSizeBytes;
		
		//System.out.println(String.format("\t\t\t\t 1 => Adding block %d->%d:%s at offset %d", 
		//		globalBlockNumber, blockNumber,Commons.uuidToString(dataBlock.uuidHigh(), dataBlock.uuidLow()), offsetInIdxBlock));
		
		Cache cache = Cache.instance();
		DataBlock indexBlock = cache.getDataBlock(uuidHigh, uuidLow);
		indexBlock.appendLong(dataBlock.uuidHigh(), offsetInIdxBlock);
		indexBlock.appendLong(dataBlock.uuidLow(), offsetInIdxBlock + 8);
	}
	
	long[] getBlockIDByByte(long offsetInFile) throws InvalidFileOffsetException{
		//TODO: Convert this recursive process in an iterative loop in the INode function.
		
		long blockInFile = offsetInFile / Constants.dataBlockSizeBytes;
		long blockInThisIndex = adjustBlockNumber(blockInFile) % Commons.maxBlocksCount(indexLevel);
		
		if (indexLevel == 1){
			int offsetInIndexBlock = (int)(blockInThisIndex) * pointerSizeBytes;
			Cache cache = Cache.instance();
			DataBlock thisIndexBlock = cache.getDataBlock(uuidHigh, uuidLow);
			long uuidHigh = thisIndexBlock.readLong(offsetInIndexBlock);
			long uuidLow = thisIndexBlock.readLong(offsetInIndexBlock+8);
			
			//System.out.println(String.format("\t\t\t\t %d <== %d : %d->%d, block uuid %s", indexLevel, offsetInFile, 
			//		blockInFile, blockInThisIndex, Commons.uuidToString(uuidHigh, uuidLow)));
			
			return new long[]{uuidHigh, uuidLow};
		}
		
		//Convert block number according to this index level.
		long blocksPerPointer = Commons.maxBlocksCount(indexLevel-1);
		long indexPointerOffset = blockInThisIndex / blocksPerPointer * pointerSizeBytes;
		
		Cache cache = Cache.instance();
		DataBlock thisIndexBlock = cache.getDataBlock(uuidHigh, uuidLow);
		
		long pointerUuidHigh = thisIndexBlock.readLong(indexPointerOffset);
		long pointerUuidLow = thisIndexBlock.readLong(indexPointerOffset+8);
		IndexBlock nextIndexBlock = new IndexBlock(pointerUuidHigh, pointerUuidLow, indexLevel-1);
		
		//System.out.println(String.format("\t\t\t\t\t %d <== %d : %d->%d, next uuid %s", indexLevel, offsetInFile, 
		//		blockInFile, blockInThisIndex, Commons.uuidToString(pointerUuidHigh, pointerUuidLow)));
		
		return nextIndexBlock.getBlockIDByByte(offsetInFile);
	}
	
	DataBlock getByTime(long timestamp){
		return null;
	}
	
	private long adjustBlockNumber(long blockNumber){
		long indirectBlocksLimit = Constants.maxDirectBlocks + Commons.maxBlocksCount(1);
		long doubleIndirectBlocksLimit = indirectBlocksLimit + Commons.maxBlocksCount(2);
		
		if (blockNumber >= doubleIndirectBlocksLimit)
			return blockNumber - doubleIndirectBlocksLimit;
		
		if (blockNumber >= indirectBlocksLimit)
			return blockNumber - indirectBlocksLimit;
		
		return blockNumber - Constants.maxDirectBlocks;
	}
	
	/*public synchronized boolean canAddBlock(){
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
	}*/
	
	long uuidHigh(){
		return uuidHigh;
	}
	
	long uuidLow(){
		return uuidLow;
	}
	
}
