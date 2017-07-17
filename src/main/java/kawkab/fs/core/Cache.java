package kawkab.fs.core;

import java.io.IOException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import kawkab.fs.commons.Constants;

public class Cache implements BlockEvictionListener {
	private static Cache instance;
	private LRUCache cache;
	private Lock cacheLock;
	private LocalProcessor syncProc;
	private volatile boolean closing = false;
	
	private Cache(){
		cache = new LRUCache(this);
		cacheLock = new ReentrantLock();
		syncProc = new LocalProcessor(Constants.syncThreadsPerDevice);
	}
	
	public static Cache instance(){
		if (instance == null) {
			instance = new Cache();
		}
		
		return instance;
	}
	
	public void createBlock(Block block) throws IOException {
		syncProc.storeLocally(block);
	}
	
	/**
	 * @param blockID
	 * @param type
	 * @return
	 * @throws IOException 
	 */
	public Block acquireBlock(BlockID blockID) throws IOException {
		if (closing)
			return null;
		
		CachedItem cached = null;
		boolean wasCached = true;
		
		cacheLock.lock();
		try { //For cached.lock()
			try { //For cacheLock.lock()
				cached = cache.get(blockID.key);
				
				//If the block is not cached
				if (cached == null){
					wasCached = false;
					Block block = blockID.newBlock();
					cached = new CachedItem(block);
					cache.put(cached.block().name(), cached);
				}
				
				//FIXME: This can stall all other threads. For example, two readers read data at the same time. 
				//The first reader gets the lock and then performs IO on the line "cached.block().loadFromDisk()". 
				//Now the other reader has the lock of cache and waiting for the first reader to unlock the cached block.
				cached.lock();  
				cached.incrementRefCnt();
			} finally {
				cacheLock.unlock();
			}
		
			if (wasCached) {
				return cached.block();
			}
			
			//cached.block().loadFromDisk(); //TODO: Make to load from the syncProc
			syncProc.load(cached.block());
		} finally {
			if (cached != null) {
				cached.unlock();
			}
		}
		
		return cached.block();
	}
	
	public void releaseBlock(BlockID blockID) {
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
			if (cached.block().dirty()) {
				try {
					syncProc.store(cached.block()); //FIXME: What to do with the exception?
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			cached.unlock();
		} finally {
			cacheLock.unlock();
		}
	}
	
	public void flush() { //
		for (CachedItem cached : cache.values()) {
			//We need to close all the readers and writers before we can empty the cache.
			//TODO: Wait until the reference count for the cached object becomes zero.
			if (cached.block().dirty()) {
				//cached.block().storeToDisk();
				try {
					syncProc.store(cached.block());
				} catch (IOException e) {
					e.printStackTrace(); //FIXME: What to do with the exception?
				}
			}
		}
	}
	
	public void shutdown() {
		System.out.println("Closing cache.");
		flush();
		cacheLock.lock();
		cache.clear();
		cacheLock.unlock();
		syncProc.stop();
	}

	@Override
	public void beforeEviction(CachedItem cachedItem) {
		Block block = cachedItem.block();
		try {
			block.waitUntilSynced();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		/*if (block.dirty()) {
			try {
				block.storeToDisk();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}*/
	}
	
}
