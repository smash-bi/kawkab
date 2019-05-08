package kawkab.fs.core;

import kawkab.fs.commons.Configuration;
import kawkab.fs.core.exceptions.KawkabException;
import kawkab.fs.utils.GCMonitor;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static kawkab.fs.core.BlockID.BlockType;

/**
 * An LRU cache for Block objects. The objects are acquired and released using BlockIDs. This class is singleton and
 * thread safe.
 */
public class BufferedCache extends Cache implements BlockEvictionListener{
	private static final Object initLock = new Object();
	private static BufferedCache instance;
	
	private Configuration conf;
	private LRUCache cache; // An extended LinkedHashMap that implements removeEldestEntry()
	private Lock cacheLock; // Cache level locking
	private LocalStoreManager localStore;
	private final int MAX_BLOCKS_IN_CACHE;
	private DSPool dsp;
	
	private BufferedCache() {
		System.out.println("Initializing cache..." );
		
		conf = Configuration.instance();
		
		cacheLock = new ReentrantLock();
		localStore = LocalStoreManager.instance();
		
		int numSegmentsInCache =
				(int)((conf.cacheSizeMiB * 1048576L)/conf.segmentSizeBytes);
		if (numSegmentsInCache <= 0) {
			System.out.println("Cache size is not sufficient to cache the metadata and the data segments");
		}
		assert numSegmentsInCache > 0;
		
		dsp = new DSPool(numSegmentsInCache);
		cache = new LRUCache(numSegmentsInCache, this);
		
		MAX_BLOCKS_IN_CACHE = numSegmentsInCache + conf.inodeBlocksPerMachine + conf.ibmapsPerMachine; //FIXME: The size of cache is not what is reflected from the configuration
	}
	
	public static BufferedCache instance() {
		if (instance == null) {
			synchronized(initLock) {
				if (instance == null) {
					instance = new BufferedCache();
				}
			}
		}
		
		return instance;
	}

	
	/**
	 * Acquires a reference of the block from the LRU cache. BlockID is an immutable ID of the block, which also creates
	 * a new block class depending on the type of the ID. The BlockID returns a unique string key that this cache uses
	 * to index the block. Therefore, if any two callers have different objects of BlockID but same data in the objects,
	 * both callers will acquire the same block.
	 * 
	 * NOTE: The caller must call releaseBlock() function to release the block and decrement its reference count, even
	 * if this function throws an exception. This need to be changed so that the caller does not need to release the
	 * block in the case of an exception. 
	 * 
	 * The function parameter createNewBlock indicates that the caller wants to create a new block. In this case, the
	 * cache creates a new block - it first reserves a block space in memory by instantiating the object through 
	 * blockID.newBlock(), and then the cache calls the localStore.createBlock() function to create the block
	 * in the local store. The local store then creates the block in the local storage.
	 * 
	 * If the block is not already in the cache, the block is brought into the cache using the block.loadBlock()
	 * function. The loadBlock function is a blocking function. Therefore, the calling thread blocks until the data
	 * is loaded into the block. The block is loaded from the local store, the global store, or the primary node of 
	 * the block.
	 * 
	 * If the block is not in memory, and adding a new block exceeds the size of the cache, the cache evicts the LRU block
	 * before creating space for the new block. The existing LRU block must have zero reference count, i.e., the block 
	 * must not be acquired by any thread. If the LRU block's reference count is not zero, it throws an exception.
	 * 
	 * An acquired block cannot be evicted from the cache. The cache keeps the reference count of all the acquired blocks.
	 * 
	 * The evicted block is first persisted to the local store if it is dirty.
	 * 
	 * Thread safety: The whole cache is locked to obtain the cached items. 
	 * If the item is already in the cache, it increments the reference count so that the count is updated atomically.
	 * If the block is not in the cache, a new block object is created and its reference count is increment. The
	 * cache is then unlocked. In this way, two simultaneous threads cannot create two different objects for the same
	 * block.
	 * 
	 * The cache lock provides mutual exclusion from the releaseBlock() function as well.
	 * 
	 * This function calls block.load() function to load data into the block.
	 * 
	 * If the function throws an exception, the caller must call the releaseBlock() function to release the block.
	 * 
	 * @param blockID
	 * @return
	 * @throws IOException The block is not cached if the exception is thrown 
	 * @throws KawkabException The block is not cached if the exception is thrown
	 */
	@Override
	public Block acquireBlock(BlockID blockID) throws IOException, KawkabException {
		// System.out.println("[C] acquire: " + blockID);
		
		CachedItem cachedItem = null;
		
		cacheLock.lock(); // Lock the whole cache. This is necessary to prevent from creating multiple references to the
		                  // same block when the block is not already cached.
		try { // To unlock the cache
			cachedItem = cache.get(blockID); // Try acquiring the block from the memory
			
			if (cachedItem == null){ // If the block is not cached
				//long t = System.nanoTime();
				Block block;
				if (blockID.type() == BlockType.DATA_SEGMENT) {
					DataSegment seg = dsp.acquire((DataSegmentID)blockID);
					seg.reset(blockID);
					block = seg;
				} else {
					block = blockID.newBlock();   // Creates a new block object to save in the cache
				}
				
				//Block block = blockID.newBlock();
				
				//t = (System.nanoTime() - t)/1000;
				//acquireStats.putValue(t);

				cachedItem = new CachedItem(block); // Wrap the object in a cached item
				cache.put(blockID, cachedItem);
			}
			
			cachedItem.incrementRefCnt();
			assert cache.size() <= MAX_BLOCKS_IN_CACHE;
		} finally {                 // FIXME: Catch any exceptions and decrement the reference count before throwing  
			cacheLock.unlock();     //        the exception. Change the caller functions to not release the block in
		                            //        the case of an exception.
		}
		
		return cachedItem.block();
	}
	
	/**
	 * Releases the block and decrements its reference count. Blocks with reference count 0 are eligible for eviction.
	 * 
	 * The released block is added in a queue for persistence if the block is dirty.
	 * 
	 * @param blockID
	 * @throws KawkabException 
	 */
	@Override
	public void releaseBlock(BlockID blockID) throws KawkabException {
		// System.out.println("[C] Release block: " + blockID);
		CachedItem cachedItem = null;
		
		cacheLock.lock(); // TODO: Do we need this lock? We may not need this lock if we change the reference counting to an AtomicInteger
		try { //For cacheLock.lock()
			cachedItem = cache.get(blockID);
			
			if (cachedItem == null) {
				System.out.println(" Releasing non-existing block: " + blockID);
				
				assert cachedItem != null; //To exit the system during testing
				return;
			}
			
			cachedItem.decrementRefCnt();
		} finally {
			cacheLock.unlock();
		}
		
		if (blockID.onPrimaryNode() && cachedItem.block().isLocalDirty()) { // Persist blocks only through the primary node
			// If the dirty bit for the local store is set
			localStore.store(cachedItem.block()); // The call is non-blocking. Multiple threads are allowed
													  // to add the same block in the queue.
		}
	}
	
	/**
	 * This function is called when the cache is full and this block is the LRU block that is being evicted from the cache.
	 * This function blocks until the local dirty count of the block becomes zero. If the local dirty count is already
	 * zero, this function returns without any wait or sleep.
	 * 
	 * The caller already has the cacheLock acquired when this function is called.
	 * 
	 * The reference count for the cachedItem is zero. Otherwise, this function cannot be called.
	 * @throws KawkabException 
	 */
	@Override
	public void beforeEviction(CachedItem cachedItem) {
		System.out.println("Evicting from cache: "+cachedItem.block().id());
		Block block = cachedItem.block();
		try {
			block.waitUntilSynced();  // FIXME: This is a blocking call and the cacheLock is locked. This may
	                                  // lead to performance problems because the thread sleeps while holding
	                                  // the cacheLock. The lock cannot be released because otherwise another
	                                  // thread can come and may acquire the block. The cache is not thread safe. So
									  // only one thread should be modifying the cache at a time.
			block.onMemoryEviction();
			localStore.notifyEvictedFromCache(cachedItem.block());
			
			if (block.id().type() == BlockType.DATA_SEGMENT)
				dsp.release((DataSegment) block);
			
		} catch (InterruptedException | KawkabException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Flushes the block in the persistent store. This should be used only for system shutdown.
	 * 
	 * @throws KawkabException
	 */
	@Override
	public void flush() throws KawkabException {
		int count = 0;
		cacheLock.lock();
		try {
			Iterator<Map.Entry<BlockID, CachedItem>> itr = cache.entrySet().iterator();
			while (itr.hasNext()) {
				//We need to close all the readers and writers before we can empty the cache.
				//TODO: Wait until the reference count for the cached object becomes zero.
				CachedItem cachedItem = itr.next().getValue();
				//beforeEviction(cachedItem);
				Block block = cachedItem.block();
				
				if (cachedItem.refCount() != 0) {
					System.err.println("  ==> Ref count is not 0: id: " + block.id() + ", count: " + cachedItem.refCount());
				}
				
				assert cachedItem.refCount() == 0;
				
				if (block.id().onPrimaryNode() && block.isLocalDirty()) {
					localStore.store(block);
					try {
						block.waitUntilSynced();
						localStore.notifyEvictedFromCache(block);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				
				if (block.id().type() == BlockType.DATA_SEGMENT) {
					dsp.release((DataSegment)block);
				}
				
				count++;
				itr.remove();
			}
			assert cache.size() == 0;
		} finally {
			cacheLock.unlock();
		}
		
		System.out.printf("Flushed %d entries from the cache. Current DSPool size is %d.\n", count, dsp.size());
	}
	
	@Override
	public void shutdown() throws KawkabException {
		System.out.println("Closing cache. Current size = "+cache.size());
		//System.out.printf("AcquireStats (us): %s\n", acquireStats);
		//System.out.printf("ReleaseStats (us): %s\n", releaseStats);
		//System.out.printf("LoadStats (us): %s\n", loadStats);
		System.out.print("GC duration stats (ms): "); GCMonitor.printStats();
		flush();
		localStore.shutdown();
	}
}
