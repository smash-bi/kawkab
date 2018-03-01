package kawkab.fs.core;

import java.io.IOException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import kawkab.fs.core.exceptions.KawkabException;

public class Cache implements BlockEvictionListener{
	private static final Object initLock = new Object(); 
	
	private static Cache instance;
	private LRUCache cache;
	private Lock cacheLock;
	private LocalStoreManager localStoreManager;
	private volatile boolean closing = false;
	
	private Cache() throws IOException{
		cacheLock = new ReentrantLock();
		localStoreManager = LocalStoreManager.instance();
		cache = new LRUCache(this);
	}
	
	public static Cache instance() throws IOException{
		if (instance == null) {
			synchronized(initLock) {
				instance = new Cache();
			}
		}
		
		return instance;
	}
	
	/*public void createBlock(Block block) throws IOException, InterruptedException {
		localStoreManager.createBlock(block);
	}*/
	
	/**
	 * An acquired block cannot be evicted from the cache. The cache keeps the
	 * reference count of all the acquired blocks.
	 * 
	 * If the function throws an exception, the caller must call the releaseBlock() function to release the block.
	 * 
	 * @param blockID
	 * @param type
	 * @return
	 * @throws IOException 
	 * @throws KawkabException 
	 * @throws InterruptedException 
	 */
	public Block acquireBlock(BlockID blockID, boolean isNewBlock) throws IOException, KawkabException, InterruptedException {
		//System.out.println("[C] acquire: " + blockID);
		
		if (closing)
			return null;
		
		CachedItem cachedItem = null;
		//boolean wasCached = true;
		
		cacheLock.lock();
		
		try { //For cacheLock.lock()
			cachedItem = cache.get(blockID.key());
			
			//If the block is not cached
			if (cachedItem == null){
				//wasCached = false;
				Block block = blockID.newBlock();
				cachedItem = new CachedItem(block);
				cache.put(cachedItem.block().id().name(), cachedItem);
			}
			
			cachedItem.incrementRefCnt();
		} finally {
			cacheLock.unlock();
		}
		
		Block block = cachedItem.block();
		if (isNewBlock) { //Not mutually exclusive because only one of the threads can create a new block because we have only one writer
			localStoreManager.createBlock(block);
		} else {
			block.loadBlock();
		}
		
		/*try { //For cachedItem.lock()
			cachedItem.lock(); //TODO: Don't acquire this lock if the cachedItem is already loaded in memory, and 
			                   //wait for the item to get loaded if the block was cached but not already loaded.
		
			if (wasCached) {
				return cachedItem.block();
			}
			
			loadBlockData(cachedItem.block());
			
			//TODO: Signal any thread that is waiting for the block to get loaded
		} finally {
			if (cachedItem != null) {
				cachedItem.unlock();
			}
		}*/
		
		return block;
	}
	
	/*private void loadBlockData(Block block) throws IOException, FileNotExistException, KawkabException {
		System.out.println("[C] Load block: This Node: " + Constants.thisNodeID + ", primary: " + block.id().primaryNodeID() + ", block: " + block.id());
		if (block.id().onPrimaryNode()) { //If this node is the primary node of the block
			localStore.load(block); // Load data from the local store
		} else {
			try {
				globalStore.load(block); // load data from the global store
			} catch (FileNotExistException e) {
				try {
					nc.getBlock(block.id(), block);
				} catch (FileNotExistException ke) {
					globalStore.load(block);
				} 
			}
		}
	}*/
	
	/**
	 * Reduces the reference count by 1. Blocks with reference count 0 are eligible for eviction.
	 * @param blockID
	 * @throws KawkabException 
	 */
	public void releaseBlock(BlockID blockID) throws KawkabException {
		//System.out.println("[C] Release block: " + blockID);
		
		CachedItem cachedItem = null;
		cacheLock.lock();
			try { //For cacheLock.lock()
				cachedItem = cache.get(blockID.key());
				
				if (cachedItem == null) {
					System.out.println(" Releasing non-existing block: " + blockID.key());
					
					assert cachedItem != null; //To exit the system during testing
					return;
				}
				
				if (blockID.onPrimaryNode()) {
					//cachedItem.lock(); try { //FIXME: Do we need a lock here???
					if (cachedItem.block().isLocalDirty()) {
						assert cachedItem.block().id().onPrimaryNode();
						
						localStoreManager.store(cachedItem.block()); // The call is non-blocking
						//FIXME: What to do with the exception?
					}
				}
				
				cachedItem.decrementRefCnt(); // No need to acquire cacheLock because the incrementRefCnt() and
				                              // decrementRefCnt() functions are only called while holding the cacheLock.
			} finally {
				cacheLock.unlock();
			}
			
			
	}
	
	public void flush() throws KawkabException { //
		for (CachedItem cached : cache.values()) {
			//We need to close all the readers and writers before we can empty the cache.
			//TODO: Wait until the reference count for the cached object becomes zero.
			if (cached.block().isLocalDirty()) {
				//cached.block().storeToDisk();
				localStoreManager.store(cached.block());
			}
		}
	}
	
	public void shutdown() throws KawkabException {
		System.out.println("Closing cache.");
		flush();
		cacheLock.lock();
		cache.clear();
		cacheLock.unlock();
		localStoreManager.stop();
	}

	/**
	 * The cache calls this function when it is evicting a block
	 */
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
