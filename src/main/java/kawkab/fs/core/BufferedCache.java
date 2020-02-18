package kawkab.fs.core;

import kawkab.fs.commons.Configuration;
import kawkab.fs.core.exceptions.KawkabException;
import kawkab.fs.core.exceptions.OutOfMemoryException;
import kawkab.fs.utils.LatHistogram;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

import static kawkab.fs.core.BlockID.BlockType;

/**
 * An LRU cache for Block objects. The objects are acquired and released using BlockIDs. This class is singleton and
 * thread safe.
 */
public class BufferedCache extends Cache implements BlockEvictionListener {
	private static final Object initLock = new Object();
	private static BufferedCache instance;

	private LRUCache cache; // An extended LinkedHashMap that implements removeEldestEntry()

	private ReentrantLock cacheLock; // Cache level locking

	//private Condition acqLockCond;
	//private Boolean canAcquireBlock = true;
	//private Block syncWaitBlock;

	private LocalStoreManager localStore;
	private final int MAX_BLOCKS_IN_CACHE;
	private DSPool dsp;

	private long evicted;
	private long accessed;
	private long missed;
	//private long waited;
	private LatHistogram acqLog;
	private LatHistogram relLog;
	private final int lowMark;
	private final int highMark;
	private final int midMark;
	private final int id;

	private BufferedCache() {
		this((int)((Configuration.instance().cacheSizeMiB * 1048576L) / Configuration.instance().segmentSizeBytes), 0,
				new DSPool((int) ((Configuration.instance().cacheSizeMiB * 1048576L) / Configuration.instance().segmentSizeBytes)));
	}

	public BufferedCache(int numSegmentsInCache, int id, DSPool dsp) {
		System.out.println("Initializing BufferedCache cache...");

		this.id = id;

		cacheLock = new ReentrantLock();
		//acqLockCond = cacheLock.newCondition();

		localStore = LocalStoreManager.instance();

		assert numSegmentsInCache > 0;

		//dsp = new DSPool(numSegmentsInCache);
		this.dsp = dsp;
		cache = new LRUCache(numSegmentsInCache, this);

		//FIXME: The size of cache is not what is reflected from the configuration
		MAX_BLOCKS_IN_CACHE = numSegmentsInCache; // + conf.inodeBlocksPerMachine + conf.ibmapsPerMachine;

		acqLog = new LatHistogram(TimeUnit.MICROSECONDS, "Cache acquire", 1, 100);
		relLog = new LatHistogram(TimeUnit.MICROSECONDS, "Cache release", 1, 100);

		highMark = (int) (numSegmentsInCache * 0.99);
		midMark = (int) (numSegmentsInCache * 0.95);
		lowMark = (int) (numSegmentsInCache * 0.90);

		System.out.printf("HM=%d, MM=%d, LM=%d\n", highMark, midMark, lowMark);

		//runEvictor();
	}

	public static BufferedCache instance() {
		if (instance == null) {
			synchronized (initLock) {
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
	 * <p>
	 * NOTE: The caller must call releaseBlock() function to release the block and decrement its reference count, even
	 * if this function throws an exception. This need to be changed so that the caller does not need to release the
	 * block in the case of an exception.
	 * <p>
	 * The function parameter createNewBlock indicates that the caller wants to create a new block. In this case, the
	 * cache creates a new block - it first reserves a block space in memory by instantiating the object through
	 * blockID.newBlock(), and then the cache calls the localStore.createBlock() function to create the block
	 * in the local store. The local store then creates the block in the local storage.
	 * <p>
	 * If the block is not already in the cache, the block is brought into the cache using the block.loadBlock()
	 * function. The loadBlock function is a blocking function. Therefore, the calling thread blocks until the data
	 * is loaded into the block. The block is loaded from the local store, the global store, or the primary node of
	 * the block.
	 * <p>
	 * If the block is not in memory, and adding a new block exceeds the size of the cache, the cache evicts the LRU block
	 * before creating space for the new block. The existing LRU block must have zero reference count, i.e., the block
	 * must not be acquired by any thread. If the LRU block's reference count is not zero, it throws an exception.
	 * <p>
	 * An acquired block cannot be evicted from the cache. The cache keeps the reference count of all the acquired blocks.
	 * <p>
	 * The evicted block is first persisted to the local store if it is dirty.
	 * <p>
	 * Thread safety: The whole cache is locked to obtain the cached items.
	 * If the item is already in the cache, it increments the reference count so that the count is updated atomically.
	 * If the block is not in the cache, a new block object is created and its reference count is increment. The
	 * cache is then unlocked. In this way, two simultaneous threads cannot create two different objects for the same
	 * block.
	 * <p>
	 * The cache lock provides mutual exclusion from the releaseBlock() function as well.
	 * <p>
	 * This function calls block.load() function to load data into the block.
	 * <p>
	 * If the function throws an exception, the caller must call the releaseBlock() function to release the block.
	 *
	 * @param blockID
	 * @return
	 * @throws IOException     The block is not cached if the exception is thrown
	 * @throws KawkabException The block is not cached if the exception is thrown
	 */

	@Override
	public Block acquireBlock(BlockID blockID) throws OutOfMemoryException {
		// System.out.println("[C] acquire: " + blockID);

		assert blockID != null;

		cacheLock.lock();    // Lock the whole cache. This is necessary to prevent from creating multiple references to the
		// same block when the block is not already cached.
		acqLog.start();

		// A block cannot be acquired if the cache has not free blocks. This can happen during peak workloads
		// when the ingestion rate is high but flushing to the localStore is slow. Therefore, we must check if the
		// block can be acquired or not. If not, (1) we should release the lock, (2) let the threads to succeed in
		// the releaseBlock function, and (3) let the thread in waitUntilSynced() to awake the threads waiting on the
		// lock condition acqLockCond.
		/*while (!canAcquireBlock) { // Cannot acquire block as the cache is full and the last evicted block is being synced to the localStore
			acqLockCond.awaitUninterruptibly();
		}*/

		accessed++;

		Block cachedBlock;
		try { // To unlock the cache
			CachedItem cachedItem = cache.get(blockID); // Try acquiring the block from the memory

			if (cachedItem == null) { // If the block is not cached
				if (cache.size() == MAX_BLOCKS_IN_CACHE-1) {
					int removed = cache.bulkRemove(1);
					//waitUntilSynced();
					if (removed == 0)
						throw new OutOfMemoryException("Cache is full. Current cache size: " + cache.size());
				}

				missed++;

				//evictIfNeeded();

				//long t = System.nanoTime();
				Block block;
				if (blockID.type() == BlockType.DATA_SEGMENT) {
					block = dsp.acquire((DataSegmentID) blockID);
				} else {
					block = blockID.newBlock();   // Creates a new block object to save in the cache

					assert block.id() != null;
				}

				cachedItem = new CachedItem(block); // Wrap the object in a cached item
				cachedBlock = block;

				cachedItem.incrementRefCnt(); //Increment the reference count before adding in the cache so that the cache cannot evict the new entry
				CachedItem prev = cache.put(blockID, cachedItem);
				assert prev == null;

				/*if (!canAcquireBlock) { // Cannot acquire the block yet as the block is evicted from the cache. We have to wait until
										// the block is synced to the local store. Otherwise, we will be overly using the memory

					assert !syncWaitBlock.id().equals(blockID);

					waitUntilSynced(syncWaitBlock, cacheLock);
					cache.remove(syncWaitBlock.id());
					onEvictBlock(syncWaitBlock); // The block is now synced. Complete the eviction process
					syncWaitBlock = null;
					canAcquireBlock = true;
					acqLockCond.signalAll();
				}*/

				assert cachedItem.block().id() != null;
			} else {
				assert cachedItem.block().id() != null;
				cachedItem.incrementRefCnt();
				cachedBlock = cachedItem.block();
			}

			assert cache.size() <= MAX_BLOCKS_IN_CACHE;
		} finally {                // FIXME: Catch any exceptions, and decrement the reference count before throwing
			int elapsed = acqLog.end(1);
			cacheLock.unlock();    // the exception. Change the caller functions to not release the block in

			if (elapsed > 10000) {
				System.out.println("Cache acquire elapsed (us): " + elapsed);
			}

			// the case of an exception.
		}

		assert cachedBlock.id() != null;

		return cachedBlock;
	}

	/*private void waitUntilSynced() throws OutOfMemoryException {
		canAcquireBlock = false;
		cacheLock.unlock();

		int evicted = evictEntries();
		if (evicted == 0) {
			try {
				localStore.syncWait();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		cacheLock.lock();

		if (cache.size() == MAX_BLOCKS_IN_CACHE) {
			throw new OutOfMemoryException("Cache is full. Current cache size: " + cache.size());
		}

		acqLockCond.signal();
		canAcquireBlock = true;
	}*/

	/**
	 * Releases the block and decrements its reference count. Blocks with reference count 0 are eligible for eviction.
	 * <p>
	 * The released block is added in a queue for persistence if the block is dirty.
	 *
	 * @param blockID
	 * @throws KawkabException
	 */
	@Override
	public void releaseBlock(BlockID blockID) {
		//System.out.println("[C] Release block: " + blockID);
		CachedItem cachedItem;

		cacheLock.lock(); // TODO: Do we need this lock? We may not need this lock if we change the reference counting to an AtomicInteger
		relLog.start();
		try { // For cacheLock.lock()
			cachedItem = cache.get(blockID);

			assert cachedItem != null : String.format("[BC] Releasing non-existing block: %s", blockID);

			cachedItem.decrementRefCnt();

			//evictIfNeeded();
		} finally {
			relLog.end(1);
			cacheLock.unlock();
		}

		if (blockID.onPrimaryNode() && cachedItem.block().isLocalDirty()) { // Persist blocks through only the primary node
			// If the dirty bit for the local store is set
			localStore.store(cachedItem.block()); // The call is non-blocking. Multiple threads are allowed
			// to add the same block in the queue.
		}
	}

	private void evictIfNeeded() {
		//int toEvict = (int)(0.025 * MAX_BLOCKS_IN_CACHE);
		int size = cache.size();
		if (size >= highMark) {
			cache.bulkRemove(size - lowMark);
		} /*else if (size >= midMark) {
			cache.bulkRemove(10000);
		}*/
	}

	private boolean evicting = false;
	int evictEntries() {
		int removed = 0;
		int size = cache.size();

		if (size > midMark)
			evicting = true;
		else if (size < lowMark)
			evicting = false;

		if (evicting) {
			try {
				cacheLock.lock();
				int toRemove = size > highMark ? 20 : size > midMark ? 15 : 5;
				removed = cache.bulkRemove(toRemove);
			} finally {
				cacheLock.unlock();
			}
		}

		return removed;
	}

	private void runEvictor() {
		Thread evictor = new Thread(() -> {
			while (true) {
				evictEntries();
				int size = cache.size();
				try {
					if (size <= lowMark)
						Thread.sleep(100);
					else if (size <= midMark)
						Thread.sleep(5);
					else
						LockSupport.parkNanos(1000000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

				/*int size = cache.size();
				double free = (double)(highMark - size)/(highMark - lowMark);
				int toSleepUs = 0;
				int maxSleepUs = 5000;

				if (free > 0) {
					toSleepUs = (int)(free * maxSleepUs);
				}

				if (toSleepUs > 5000)
					toSleepUs = 5000;

				try {
					if (toSleepUs > 3000)
						Thread.sleep(toSleepUs/1000);
					else
						LockSupport.parkNanos(toSleepUs*1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}*/

			}
		});
		evictor.setName("CacheEvictor");
		evictor.setDaemon(true);
		evictor.start();
	}

	/**
	 * This function is called when a the block is selected to be evicted from the cache.
	 * <p>
	 * Pre-condition: The cachedItem.referenceCount must be zero
	 */
	@Override
	public void beforeEviction(CachedItem cachedItem) {
		//System.out.println("Before eviction: " + cachedItem.block().id());

		/*assert cacheLock.isHeldByCurrentThread();
		assert cachedItem.refCount() == 0;

		syncWaitBlock = cachedItem.block();
		canAcquireBlock = false;*/

		assert false;
	}

	// Perform the necessary actions to evict the block
	@Override
	public void onEvictBlock(Block block) {
		//assert cacheLock.isHeldByCurrentThread();
		assert !block.isLocalDirty();

		evicted++;

		//try {
		//localStore.notifyEvictedFromCache(block);

		if (block.id().type() == BlockType.DATA_SEGMENT) {
			dsp.release((DataSegment) block);
		}
		//catch (KawkabException e) {
		//	e.printStackTrace();
		//}
	}

	/**
	 * This function blocks until the local dirty count of the block becomes zero. If the local dirty count is already
	 * zero, this function returns without any wait or sleep.
	 *
	 * The caller already has the cacheLock acquired when this function is called.
	 * @param block THe block for which the current thread to wait until the block is synced to the localStore
	 */
	/*private void waitUntilSynced(Block block, Lock lock) {
		// The caller has acquired the cacheLock in the acquireBlock function. We should (1) set canAcquire to
		// false to prevent further blocks from

		//System.out.println("Sync wait: " + block.id());

		try {
			lock.unlock();	// Release the lock so that the threads can releaseBlocks
			block.waitUntilSynced();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			lock.lock(); // We must acquire the lock again before returning
		}
	}*/

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

				assert cachedItem.refCount() == 0 : String.format("refCount is %d for node %s while flushing", cachedItem.refCount(), block.id());

				if (block.id().onPrimaryNode() && block.isLocalDirty()) {
					localStore.store(block);
					try {
						block.waitUntilSynced();
						//localStore.notifyEvictedFromCache(block);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}

				if (block.id().type() == BlockType.DATA_SEGMENT) {
					dsp.release((DataSegment) block);
				}

				count++;
				//itr.remove(); // This function removes the item from the cache
			}
			cache.clear();
		} finally {
			cacheLock.unlock();
		}

		System.out.printf("Flushed %d entries from the cache. Current cache size is %d.\n", count, cache.size());
	}

	@Override
	public void shutdown() throws KawkabException {
		System.out.println("Closing BufferedCache cache. Current size = " + cache.size());
		System.out.println(getStats());
		//System.out.printf("AcquireStats (us): %s\n", acquireStats);
		//System.out.printf("ReleaseStats (us): %s\n", releaseStats);
		//System.out.printf("LoadStats (us): %s\n", loadStats);
		flush();
		localStore.shutdown();
	}

	@Override
	public long size() {
		/*cacheLock.lock();
		try {
			return cache.size();
		} finally {
			cacheLock.unlock();
		}*/
		return cache.size();
	}

	@Override
	public String getStats() {
		if (accessed == 0)
			return "No stats";

		return String.format("filled=%.2f%%, accessed=%d, missed=%d, evicted=%d, hitRatio=%.02f\nacqLog: %s\nrelLog: %s\n%s\n",
				cache.size() * 100.0 / MAX_BLOCKS_IN_CACHE,
				accessed, missed, evicted, 100.0 * (accessed - missed) / accessed, acqLog.getStats(), relLog.getStats(), cache.getStats());

	}

	@Override
	public void printStats() {
		System.out.println(getStats());
	}

	@Override
	public void resetStats() {
		accessed = missed = evicted = 0;
		acqLog.reset();
		relLog.reset();
		cache.resetStats();
	}


	long evictCount() {
		return evicted;
	}

	long missCount() {
		return missed;
	}

	long accessCount() {
		return accessed;
	}

	/*private void runEvictor() {
		Thread evictor = new Thread(new Evictor());
		evictor.setName("BufCacheEvictor-"+id);
		evictor.setDaemon(true);
		evictor.start();
	}

	private class Evictor implements Runnable {
		@Override
		public void run() {
			midMarkPolicy3();
		}

		private void midMarkPolicy1() {
			while(true) {
				if (cache.size() > midMark) {
					int size;
					while((size = cache.size()) > lowMark) {
						cacheLock.lock();
						try {
							int toEvict = size > highMark ? size - lowMark : 1000;
							cache.bulkRemove(toEvict);
						} finally {
							cacheLock.unlock();
						}
					}
				}

				try {
					Thread.sleep(250);
				} catch (InterruptedException e) {
					e.printStackTrace();
					break;
				}
			}
		}

		private void midMarkPolicy2() {
			while(true) {
				if (cache.size() > midMark) {
					int size;
					while((size = cache.size()) > lowMark) {
						cacheLock.lock();
						try {
							int toEvict = size > highMark ? size - lowMark : 10;
							cache.bulkRemove(toEvict);
						} finally {
							cacheLock.unlock();
						}
					}
				}

				try {
					Thread.sleep(250);
				} catch (InterruptedException e) {
					e.printStackTrace();
					break;
				}
			}
		}

		private void midMarkPolicy3() {
			while(true) {
				if (cache.size() > midMark) {
					int size;
					while((size = cache.size()) > lowMark) {
						cacheLock.lock();
						try {
							int toEvict = size > highMark ? size - lowMark : 10;
							cache.bulkRemove(toEvict);
						} finally {
							cacheLock.unlock();
						}

						try {
							Thread.sleep(250);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
				}

				try {
					Thread.sleep(250);
				} catch (InterruptedException e) {
					e.printStackTrace();
					break;
				}
			}
		}
	}*/
}
