package kawkab.fs.core;

import java.io.IOException;
import java.util.Deque;
import java.util.LinkedList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Cache{
	private static Cache instance;
	private LocalStore store;
	private volatile boolean stop;
	private LRUCache cache;
	private Lock cacheLock;
	private Deque<CachedItem> evictedItems;
	
	private Cache(){
		store = LocalStore.instance();
		evictedItems = new LinkedList<CachedItem>();
		cache = new LRUCache(evictedItems);
		cacheLock = new ReentrantLock();
		
		//dirtyBlocks = new LinkedBlockingQueue<Block>();
		//runBlocksWriter();
	}
	
	public static Cache instance(){
		if (instance == null) {
			instance = new Cache();
		}
		
		return instance;
	}
	
	public DataBlock newDataBlock(){
		return (DataBlock)acquireBlock(null, true);
	}
	
//	private DataBlock createDataBlock(){
//		UUID uuid = UUID.randomUUID();
//		long uuidHigh = uuid.getMostSignificantBits();
//		long uuidLow = uuid.getLeastSignificantBits();
//		DataBlock block = new DataBlock(new BlockID(uuidHigh, uuidLow, DataBlock.name(uuidHigh, uuidLow), BlockType.DataBlock));
//		
//		//We should add the block in cache and skip writing to L2 cache here.
//		/*try {
//			store.writeBlock(block);
//		} catch (IOException e) {
//			e.printStackTrace();
//		}*/
//		
//		return block;
//	}
	
	
	/**
	 * Pre-condition: The block must already exist in the system. This function does not create a 
	 * new block. If the block is not in cache, this function reads the block from next level
	 * storate and puts the block in the cache.
	 * 
	 * @param blockID
	 * @param type
	 * @return
	 */
	public Block acquireBlock(BlockID blockID){
		return acquireBlock(blockID, false);
	}
	
	private Block acquireBlock(BlockID blockID, boolean newDataBlock){
		CachedItem cached = null;
		boolean wasCached = true;
		
		cacheLock.lock();
		try {
			if (!newDataBlock) {
				cached = cache.get(blockID.key);
			}
			
			//If the block is not cached
			if (cached == null){
				wasCached = false;
				if (newDataBlock){
					blockID = DataBlock.randomID();
				}
				
				Block block = Block.newBlock(blockID);
				cached = new CachedItem(block);
				cache.put(block.name(), cached);
			}
			cached.lock();
		} finally {
			cacheLock.unlock();
		}
		
		cached.incrementRefCnt();
		
		if (wasCached) {
			cached.unlock(); //FIXME: Should it  not be in the finally block???
			return cached.block();
		}
		
		CachedItem toEvict = evictedItems.poll(); //FIXME: We need a dedicated evictor that writes to L2 if the evicted block is dirty
		
		if (toEvict != null && toEvict.block().dirty()) {
			try {
				//FIXME: This should be done by another thread.
				store.writeBlock(toEvict.block());
			} catch (IOException e) {
				e.printStackTrace(); //FIXME: How to handle this exception? Should throw or what???
			}
		}
		
		if (!newDataBlock) {
			try {
				store.readBlock(cached.block());
			} catch (IOException e) {
				e.printStackTrace(); //FIXME: How to handle this exception? Should throw or what???
			}
		}
		
		cached.unlock(); //FIXME: Should it  not be in the finally block???
		
		return cached.block();
	}
	
	public void releaseBlock(BlockID blockID){
		CachedItem cached = null;
		
		cacheLock.lock();
		try {
			cached = cache.get(blockID.key);
			assert cached != null;
			cached.lock();
			cached.decrementRefCnt();
			cached.unlock();
		} finally {
			cacheLock.unlock();
		}
	}
	
	public void flush(){
		cacheLock.lock();
		for (CachedItem cached : cache.values()){
			if (cached.block().dirty()){
				try {
					store.writeBlock(cached.block());
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		cacheLock.unlock();
	}
	
	public void shutdown(){
		System.out.println("Closing cache.");
		stop = true;
		
		flush();
		
		cacheLock.lock();
		cache.clear();
		cacheLock.unlock();
		
		/*Block block = null;
		while((block=dirtyBlocks.poll()) != null){
			if (block.dirty()){
				try {
					store.writeBlock(block);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}*/
	}
}
