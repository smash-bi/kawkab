package kawkab.fs.core;

import java.io.IOException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import kawkab.fs.commons.Constants;

public class Cache implements BlockEvictionListener{
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
		
		CachedItem cachedItem = null;
		boolean wasCached = true;
		
		cacheLock.lock();
		try { //For cachedItem.lock()
			try { //For cacheLock.lock()
				cachedItem = cache.get(blockID.key);
				
				//If the block is not cached
				if (cachedItem == null){
					wasCached = false;
					Block block = blockID.newBlock();
					cachedItem = new CachedItem(block);
					cache.put(cachedItem.block().name(), cachedItem);
				}
				
				//FIXME: This can stall all other threads. For example, two readers read data at the same time. 
				//The first reader gets the lock and then performs IO on the line "csyncProc.load(cachedItem.block())". 
				//Now the other reader has the lock of cache and waiting for the first reader to unlock the cached block.
				cachedItem.incrementRefCnt();
			} finally {
				cacheLock.unlock();
			}
			
			cachedItem.lock(); //TODO: Don't acquire this lock if the cachedItem is already loaded in memory
		
			if (wasCached) {
				//TODO: Wait for the item to get loaded if it is not already loaded
				return cachedItem.block();
			}
			
			syncProc.load(cachedItem.block());
			
			//TODO: Signal any thread that is waiting for the block to get loaded
		} finally {
			if (cachedItem != null) {
				cachedItem.unlock();
			}
		}
		
		return cachedItem.block();
	}
	
	public void releaseBlock(BlockID blockID) {
		CachedItem cachedItem = null;
		cacheLock.lock();
		//try { //For cachedItem.lock()
			try { //For cacheLock.lock()
				cachedItem = cache.get(blockID.key);
				
				if (cachedItem == null) {
					System.out.println(" Releasing non-existing block: " + blockID.key);
				}
				
				assert cachedItem != null;
				cachedItem.decrementRefCnt();
			} finally {
				cacheLock.unlock();
			}
			
			//cachedItem.lock(); //FIXME: Do we need a lock here???
			if (cachedItem.block().dirty()) {
				try {
					syncProc.store(cachedItem.block()); //FIXME: What to do with the exception?
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		//} finally {
		//	cachedItem.unlock();
		//}
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
			block.waitUntilSynced(); //FIXME: This is called while holding the cacheLock and the thread may sleep while holding the lock.
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
