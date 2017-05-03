package kawkab.fs.core;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Cache implements BlockEvictionListener {
	private static Cache instance;
	private volatile boolean stop;
	private LRUCache cache;
	private Lock cacheLock;
	//private BlockingQueue<CachedItem> evictedItems;
	private Thread evictor;
	//private GuavaCache gcache;
	
	private Cache(){
		//evictedItems = new LinkedBlockingQueue<CachedItem>();
		cache = new LRUCache(this);
		cacheLock = new ReentrantLock();
		
		//gcache = GuavaCache.instance();
		
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
	 * @param blockID
	 * @param type
	 * @return
	 */
	public Block acquireBlock(BlockID blockID){
		return acquireBlock(blockID, false);
	}
	
	private Block acquireBlock(BlockID blockID, boolean newDataBlock){
		//long time = System.nanoTime();
		
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
				if (newDataBlock) {
					blockID = DataBlock.randomID();
				}
				
				Block block = Block.newBlock(blockID);
				cached = new CachedItem(block);
				cache.put(cached.block().name(), cached);
			} 
			cached.lock();
			
			if (!newDataBlock) {
				cached.incrementRefCnt();
			}
		} finally {
			cacheLock.unlock();
		}
		
		if (wasCached) {
			cached.unlock(); //FIXME: Should it  not be in the finally block???
			return cached.block();
		}
		
		/*CachedItem toEvict = evictedItems.poll(); //FIXME: We need a dedicated evictor that writes to L2 if the evicted block is dirty
		if (toEvict != null && toEvict.block().dirty()) {
			try {
				//FIXME: This should be done by another thread.
				store.writeBlock(toEvict.block());
			} catch (IOException e) {
				e.printStackTrace(); //FIXME: How to handle this exception? Should throw or what???
			}
		}*/
		
		if (!newDataBlock) {
			cached.block().loadFromDisk();
			
			/*try {
				store.readBlock(cached.block());
			} catch (IOException e) {
				e.printStackTrace(); //FIXME: How to handle this exception? Should throw or what???
			}*/
		} else {
			//Create new file.
		}
		
		cached.unlock(); //FIXME: Should it  not be in the finally block???
		
		//time = System.nanoTime() - time;
		//System.out.println(time/1000);
		
		return cached.block();
	}
	
	public void releaseBlock(BlockID blockID){
		CachedItem cached = null;
		
		cacheLock.lock();
		try {
			cached = cache.get(blockID.key);
			
			if (cached == null) {
				System.out.println(" Releasing non-existing block: " + blockID.key);
				new Exception().printStackTrace();
			}
			
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
				cached.block().storeToDisk();
				
				/*try {
					store.writeBlock(cached.block());
				} catch (IOException e) {
					e.printStackTrace();
				}*/
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
	}

	@Override
	public void beforeEviction(CachedItem cachedItem) {
		Block block = cachedItem.block();
		if (block.dirty()){
			block.storeToDisk();
		}
	}
	
	/*public void runEvictor(){
		evictor = new Thread(){
			public void run(){
				while(!stop) {
					try {
						CachedItem item = evictedItems.take();
						if (item.block().dirty()){
							store.writeBlock(block);
						}
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		};
		
		evictor.start();
	}*/
}
