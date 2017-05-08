package kawkab.fs.core;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Cache implements BlockEvictionListener {
	private static Cache instance;
	private LRUCache cache;
	private Lock cacheLock;
	
	private Cache(){
		cache = new LRUCache(this);
		cacheLock = new ReentrantLock();
	}
	
	public static Cache instance(){
		if (instance == null) {
			instance = new Cache();
		}
		
		return instance;
	}
	
	/**
	 * @param blockID
	 * @param type
	 * @return
	 */
	public Block acquireBlock(BlockID blockID){
		CachedItem cached = null;
		boolean wasCached = true;
		
		cacheLock.lock();
		try {
			cached = cache.get(blockID.key);
			
			//If the block is not cached
			if (cached == null){
				wasCached = false;
				Block block = blockID.newBlock();
				cached = new CachedItem(block);
				cache.put(cached.block().name(), cached);
			}
			
			//FIXME: This can stall all other threads. For example, two readers read data at the same time. 
			//The first reader gets the lock and then performs IO on line "cached.block().loadFromDisk()". 
			//Now the other reader has the lock of cache and waiting for the first reader to unlock the cached block.
			cached.lock();  
			cached.incrementRefCnt();
		} finally {
			cacheLock.unlock();
		}
		
		if (wasCached) {
			cached.unlock(); //FIXME: Should it  not be in the finally block???
			return cached.block();
		}
		
		cached.block().loadFromDisk();
		cached.unlock(); //FIXME: It should be in the finally block???
		
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
			cached.lock(); //FIXME: We lock the cached object only to increment and decrement reference count. Why not to use AtomicInteger for that purpose?
			cached.decrementRefCnt();
			cached.unlock();
		} finally {
			cacheLock.unlock();
		}
	}
	
	public void flush(){
		cacheLock.lock();
		for (CachedItem cached : cache.values()) {
			if (cached.block().dirty()) {
				cached.block().storeToDisk();
			}
		}
		cacheLock.unlock();
	}
	
	public void shutdown(){
		System.out.println("Closing cache.");
		
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
}
