package kawkab.fs.core;

import java.io.IOException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import kawkab.fs.commons.Constants;
import kawkab.fs.core.exceptions.FileNotExistException;
import kawkab.fs.core.exceptions.KawkabException;
import kawkab.fs.core.services.PrimaryNodeServiceClient;

public class Cache implements BlockEvictionListener{
	private static Cache instance;
	private LRUCache cache;
	private Lock cacheLock;
	private LocalProcessor localStore;
	private GlobalProcessor globalStore;
	private PrimaryNodeServiceClient nc;
	private volatile boolean closing = false;
	
	private Cache(){
		cache = new LRUCache(this);
		cacheLock = new ReentrantLock();
		localStore = LocalProcessor.instance();
		globalStore = GlobalProcessor.instance();
		nc = PrimaryNodeServiceClient.instance();
	}
	
	public static Cache instance(){
		if (instance == null) {
			instance = new Cache();
		}
		
		return instance;
	}
	
	public void createBlock(Block block) throws IOException {
		localStore.storeLocally(block);
	}
	
	/**
	 * An acquired block cannot be evicted from the cache. The cache keeps the
	 * reference count of all the acquired blocks.
	 * 
	 * @param blockID
	 * @param type
	 * @return
	 * @throws IOException 
	 * @throws KawkabException 
	 */
	public Block acquireBlock(BlockID blockID) throws IOException, KawkabException {
		System.out.println("[C] acquire: " + blockID);
		
		if (closing)
			return null;
		
		CachedItem cachedItem = null;
		//boolean wasCached = true;
		
		cacheLock.lock();
		
		try { //For cacheLock.lock()
			cachedItem = cache.get(blockID.key);
			
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
		block.loadBlock();
		
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
		System.out.println("[C] Release block: " + blockID);
		
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
			
			if (blockID.onPrimaryNode()) {
				//cachedItem.lock(); try { //FIXME: Do we need a lock here???
				if (cachedItem.block().dirty()) {
					localStore.store(cachedItem.block()); //FIXME: What to do with the exception?
					
				}
				//} finally {
				//	cachedItem.unlock();
				//}
			}
	}
	
	public void flush() throws KawkabException { //
		for (CachedItem cached : cache.values()) {
			//We need to close all the readers and writers before we can empty the cache.
			//TODO: Wait until the reference count for the cached object becomes zero.
			if (cached.block().dirty()) {
				//cached.block().storeToDisk();
				localStore.store(cached.block());
			}
		}
	}
	
	public void shutdown() throws KawkabException {
		System.out.println("Closing cache.");
		flush();
		cacheLock.lock();
		cache.clear();
		cacheLock.unlock();
		localStore.stop();
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
