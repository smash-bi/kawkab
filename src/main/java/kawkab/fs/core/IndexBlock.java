package kawkab.fs.core;

import java.io.IOException;

import kawkab.fs.commons.Commons;
import kawkab.fs.commons.Constants;
import kawkab.fs.core.Block.BlockType;
import kawkab.fs.core.exceptions.IndexBlockFullException;
import kawkab.fs.core.exceptions.InvalidFileOffsetException;

public class IndexBlock {
	public static final int pointerSizeBytes = 16; //Two long values: uuidHigh and uuidLow
	
	//private long uuidHigh;
	//private long uuidLow;
	private final int indexLevel;
	private final BlockID uuid; //UUID of the dataBlock of this index
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
	
	IndexBlock(BlockID uuid, int indexLevel){
		assert indexLevel > 0;
		this.uuid = uuid;
		this.indexLevel = indexLevel;
	}
	
	/**
	 * @param dataBlock DataBlock to be added in the index
	 * @param blockNumber zero based block number of the new block
	 * @throws IndexBlockFullException
	 * @throws InvalidFileOffsetException 
	 * @throws IOException 
	 */
	synchronized void addBlock(BlockID dataBlockID, long blockNumber) throws IndexBlockFullException, InvalidFileOffsetException, IOException{
		/*if (!canAddBlock()){
			throw new IndexBlockFullException("Cannot add a new block.");
		}*/
		
		if (indexLevel == 1) {
			appendDataBlock(dataBlockID, blockNumber);
			return;
		}
		
		//Convert block number according to this index level.
		long blockInThisIndex = blockInSubtree(blockNumber) % Commons.maxBlocksCount(indexLevel);
		long blocksPerPointer = Commons.maxBlocksCount(indexLevel-1);
		long indexPointer = blockInThisIndex / blocksPerPointer;
		
		Cache cache = Cache.instance();
		try(DataBlock thisIndexBlock = (DataBlock)cache.acquireBlock(uuid)) { //Every index block closes its own data block and flushes to disk.
			int offsetInBlock = (int)indexPointer * pointerSizeBytes;
			long pointerUuidHigh = thisIndexBlock.readLong(offsetInBlock);
			long pointerUuidLow = thisIndexBlock.readLong(offsetInBlock+8);
			
			IndexBlock nextIndexBlock = null;
			if (pointerUuidHigh == 0 && pointerUuidLow == 0){
				//DataBlock indexBlock = cache.newDataBlock();
				BlockID indexBlockID = DataBlock.createNewBlock();
				nextIndexBlock = new IndexBlock(indexBlockID, indexLevel-1);
				thisIndexBlock.appendLong(nextIndexBlock.uuid.uuidHigh, offsetInBlock);
				thisIndexBlock.appendLong(nextIndexBlock.uuid.uuidLow, offsetInBlock+8);
			}else{
				BlockID pointerBlockID = new BlockID(pointerUuidHigh, pointerUuidLow, DataBlock.name(pointerUuidHigh, pointerUuidLow), BlockType.DataBlock);
				nextIndexBlock = new IndexBlock(pointerBlockID, indexLevel-1);
			}
			
			//System.out.println(String.format("\t\t\t %d => Adding block %d->%d, indexPointer %d, next uuid %s", 
			//		indexLevel, blockNumber, blockInThisIndex, indexPointer, Commons.uuidToString(nextIndexBlock.uuidHigh, nextIndexBlock.uuidLow)));
			
			nextIndexBlock.addBlock(dataBlockID, blockNumber);
		}
	}
	
	synchronized void appendDataBlock(BlockID dataBlockID, long globalBlockNumber) throws IndexBlockFullException{
		assert indexLevel == 1;
		
		int blockNumber = (int)(blockInSubtree(globalBlockNumber) % Commons.maxBlocksCount(1));
		int offsetInIdxBlock = (int)(blockNumber) * pointerSizeBytes;
		
		//System.out.println(String.format("\t\t\t\t 1 => Adding block %d->%d:%s at offset %d", 
		//		globalBlockNumber, blockNumber,Commons.uuidToString(dataBlock.uuidHigh(), dataBlock.uuidLow()), offsetInIdxBlock));
		
		Cache cache = Cache.instance();
		try (DataBlock thisIndexBlock = (DataBlock)cache.acquireBlock(uuid)) {
			thisIndexBlock.appendLong(dataBlockID.uuidHigh, offsetInIdxBlock);
			thisIndexBlock.appendLong(dataBlockID.uuidLow, offsetInIdxBlock + 8);
		}
	}
	
	BlockID getBlockIDByByte(long offsetInFile) throws InvalidFileOffsetException, IOException{
		//TODO: Convert this recursive process in an iterative loop in the INode function.
		
		long blockInFile = offsetInFile / Constants.dataBlockSizeBytes;
		long blockInThisIndex = blockInSubtree(blockInFile) % Commons.maxBlocksCount(indexLevel);
		
		if (indexLevel == 1){
			int offsetInIndexBlock = (int)(blockInThisIndex) * pointerSizeBytes;
			Cache cache = Cache.instance();
			long uuidHigh = 0;
			long uuidLow = 0;
			try(DataBlock thisIndexBlock = (DataBlock)cache.acquireBlock(uuid)) {
				uuidHigh = thisIndexBlock.readLong(offsetInIndexBlock);
				uuidLow = thisIndexBlock.readLong(offsetInIndexBlock+8);
			}
			
			//System.out.println(String.format("\t\t\t\t %d <== %d : %d->%d, block uuid %s", indexLevel, offsetInFile, 
			//		blockInFile, blockInThisIndex, Commons.uuidToString(uuidHigh, uuidLow)));
			
			return new BlockID(uuidHigh, uuidLow, DataBlock.name(uuidHigh, uuidLow), BlockType.DataBlock);
		}
		
		//Convert block number according to this index level.
		long blocksPerPointer = Commons.maxBlocksCount(indexLevel-1);
		long indexPointerOffset = blockInThisIndex / blocksPerPointer * pointerSizeBytes;
		
		Cache cache = Cache.instance();
		long pointerUuidHigh = 0;
		long pointerUuidLow = 0;
		try (DataBlock thisIndexBlock = (DataBlock)cache.acquireBlock(uuid)) {
			pointerUuidHigh = thisIndexBlock.readLong(indexPointerOffset);
			pointerUuidLow = thisIndexBlock.readLong(indexPointerOffset+8);
		}
		BlockID pointerBlockID = new BlockID(pointerUuidHigh, pointerUuidLow, DataBlock.name(pointerUuidHigh, pointerUuidLow), BlockType.DataBlock);
		IndexBlock nextIndexBlock = new IndexBlock(pointerBlockID, indexLevel-1);
		
		//System.out.println(String.format("\t\t\t\t\t %d <== %d : %d->%d, next uuid %s", indexLevel, offsetInFile, 
		//		blockInFile, blockInThisIndex, Commons.uuidToString(pointerUuidHigh, pointerUuidLow)));
		
		return nextIndexBlock.getBlockIDByByte(offsetInFile);
	}
	
	DataBlock getByTime(long timestamp){
		return null;
	}
	
	private long blockInSubtree(long blockNumber){
		long indirectBlocksLimit = Constants.directBlocksPerInode + Commons.maxBlocksCount(1);
		long doubleIndirectBlocksLimit = indirectBlocksLimit + Commons.maxBlocksCount(2);
		
		if (blockNumber >= doubleIndirectBlocksLimit)
			return blockNumber - doubleIndirectBlocksLimit;
		
		if (blockNumber >= indirectBlocksLimit)
			return blockNumber - indirectBlocksLimit;
		
		return blockNumber - Constants.directBlocksPerInode;
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
	
	BlockID uuid(){
		return uuid;
	}
}
