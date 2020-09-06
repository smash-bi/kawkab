package kawkab.fs.core;

import kawkab.fs.commons.Configuration;
import kawkab.fs.core.exceptions.KawkabException;
import kawkab.fs.core.exceptions.OutOfMemoryException;
import kawkab.fs.utils.Accumulator;
import kawkab.fs.utils.GCMonitor;
import kawkab.fs.utils.LatHistogram;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.LockSupport;

import static kawkab.fs.core.BlockID.BlockType;

/**
 * An LRU cache for Block objects. The objects are acquired and released using BlockIDs. This class is singleton and
 * thread safe.
 */
public class PartitionedBufferedCache extends Cache {
	private static final Object initLock = new Object();
	private static PartitionedBufferedCache instance;
	
	private Configuration conf;
	private BufferedCache[] cache; // An extended LinkedHashMap that implements removeEldestEntry()
	private LocalStoreManager localStore;
	private int numPartitions = 12;

	private ConcurrentHashMap<BlockID, CachedItem> pinnedMap;
	private KeyedLock<Integer> pinLock;
	private int totalSegments;
	private volatile boolean working = true;
	private DSPool dsp;

	private PartitionedBufferedCache() {
		System.out.println("Initializing PartitionedBufferedCache cache..." );
		
		conf = Configuration.instance();
		
		localStore = LocalStoreManager.instance();
		
		totalSegments = (int)((conf.cacheSizeMiB * 1048576L)/conf.segmentSizeBytes);
		if (totalSegments <= 0) {
			System.out.println("Cache size is not sufficient to cache the metadata and the data segments");
		}
		assert totalSegments > 0;

		dsp = new DSPool(totalSegments+1);
		int numSegmentsPerPart = totalSegments/numPartitions;
		
		cache = new BufferedCache[numPartitions];
		for (int i=0; i<numPartitions; i++) {
			cache[i] = new BufferedCache(numSegmentsPerPart, i+1, dsp);
		}

		pinnedMap = new ConcurrentHashMap<>();
		pinLock = new KeyedLock<>();

		runEvictor();
	}
	
	public static PartitionedBufferedCache instance() {
		if (instance == null) {
			synchronized(initLock) {
				if (instance == null) {
					instance = new PartitionedBufferedCache();
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
	public Block acquireBlock(BlockID blockID)  throws OutOfMemoryException {
		if (blockID.type() != BlockType.DATA_SEGMENT) {
			return acquirePinned(blockID);
		}
		// System.out.println("[C] acquire: " + blockID);

		int numPart = blockID.hashCode() % numPartitions;
		return cache[numPart].acquireBlock(blockID);
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
	public void releaseBlock(BlockID blockID) {
		if (blockID.type() != BlockType.DATA_SEGMENT) {
			releasePinned(blockID);
			return;
		}

		// System.out.println("[C] Release block: " + blockID);
		int numPart = blockID.hashCode()%numPartitions;
		cache[numPart].releaseBlock(blockID);
	}

	private Block acquirePinned(BlockID blockID) {
		CachedItem ci = pinnedMap.get(blockID);
		int hc = blockID.hashCode();
		if (ci == null) {
			//A lock is required to prevent concurrent additions of different memory blocks for the same blockID
			pinLock.lock(hc);  // Lock based on the blockID

			try{
				if ((ci = pinnedMap.get(blockID)) == null) {
					ci = new CachedItem(blockID.newBlock());
					pinnedMap.put(blockID, ci);
				}
			} finally {
				pinLock.unlock(hc);
			}
		}

		ci.incrementRefCnt();

		return ci.block();
	}

	private void releasePinned(BlockID blockID) {
		CachedItem ci = pinnedMap.get(blockID);

		assert ci != null : "Releasing non-cached item: " + blockID;

		int rc = ci.decrementRefCnt();

		Block block = ci.block();
		if (blockID.onPrimaryNode() && block.isLocalDirty()) {
			localStore.store(block);
		}

		//FIXME: Remove the block if its refCount is zero and its bytes are not dirty
		/*if (rc == 0) {
			pinLock.lock(blockID.hashCode());
			try{
				if (ci.refCount() > 0)
					return;

				pinnedMap.remove(blockID);
			} finally {
				pinLock.unlock(blockID.hashCode());
			}
		}*/
	}
	
	/**
	 * Flushes the block in the persistent store. This should be used only for system shutdown.
	 * 
	 * @throws KawkabException
	 */
	@Override
	public void flush() throws KawkabException {
		for (int i=0; i<numPartitions; i++) {
			cache[i].flush();
		}

		flushPinned();
	}

	private void flushPinned() throws KawkabException {
		for (CachedItem cachedItem : pinnedMap.values()) {
			Block block = cachedItem.block();
			if (block.id().onPrimaryNode() && block.isLocalDirty()) {
				localStore.store(block);
				try {
					block.waitUntilSynced();
					//localStore.notifyEvictedFromCache(block);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}

			if (cachedItem.refCount() != 0) {
				System.out.println("Ref count is not 0: id: " + block.id() + ", count: " + cachedItem.refCount());
			} else {
				pinnedMap.remove(block.id());
			}

			/*assert cachedItem.refCount() == 0;*/
		}

		//pinnedMap.clear();
	}
	
	@Override
	public void shutdown() throws KawkabException {
		working = false;

		int size = pinnedMap.size();
		for (int i=0; i<numPartitions; i++) {
			size += cache[i].size();
			cache[i].shutdown();
		}

		System.out.println("Closing PartitionedBufferedCache cache. Current size = "+size);
		//System.out.printf("AcquireStats (us): %s\n", acquireStats);
		//System.out.printf("ReleaseStats (us): %s\n", releaseStats);
		//System.out.printf("LoadStats (us): %s\n", loadStats);
		System.out.print("GC duration stats (ms): "); GCMonitor.printStats();
		flush();
		//flushPinned();
		localStore.shutdown();
	}
	
	@Override
	public long size() {
		int size = pinnedMap.size();
		for (int i=0; i<numPartitions; i++) {
			size += cache[i].size();
		}
		return size;
	}

	@Override
	public String getStats() {
		long evicted = 0;
		long missed = 0;
		long accessed = 0;
		long size = 0;

		StringBuilder stats = new StringBuilder();
		LatHistogram acqStatsAgg = null;
		LatHistogram relStatsAgg = null;
		for (int i=0; i<numPartitions; i++) {
			//stats.append(cache[i].getStats());
			if (i == 0) {
				acqStatsAgg = cache[i].acqStats();
				relStatsAgg = cache[i].relStats();
			} else {
				acqStatsAgg.merge(cache[i].acqStats());
				relStatsAgg.merge(cache[i].relStats());
			}

			evicted += cache[i].evictCount();
			missed += cache[i].missCount();
			accessed += cache[i].accessCount();
			//waited += cache[i].waitCount();
			size += cache[i].size();
		}

		if (accessed > 0)
			stats.append(String.format("Agg acquire: %s\nAgg release: %s\nCache agg: size=%.0f%%, accessed=%d, missed=%d, evicted=%d, hitRatio=%.02f\n",
					acqStatsAgg.getStats(), relStatsAgg.getStats(), size*100.0/totalSegments, accessed, missed, evicted, 100.0*(accessed-missed)/accessed));

		System.out.println("Pinned map size: " + pinnedMap.size());

		return stats.toString();
	}

	@Override
	public void printStats(){
		for (int i=0; i<numPartitions; i++) {
			cache[i].printStats();
		}

		LatHistogram acqStatsAgg = cache[0].acqStats();
		LatHistogram relStatsAgg = cache[0].relStats();
		for (int i=1; i<numPartitions; i++) {
			acqStatsAgg.merge(cache[i].acqStats());
			relStatsAgg.merge(cache[i].relStats());
		}

		System.out.printf("Agg acquire stats: %s\n", acqStatsAgg.getStats());
		System.out.printf("Agg release stats: %s\n", acqStatsAgg.getStats());
	}

	@Override
	public void resetStats() {
		for (int i=0; i<numPartitions; i++) {
			cache[i].resetStats();
		}
	}

	private void runEvictor() {
		final int numThreads = 1;
		final int partsPerThr = numPartitions/numThreads;

		for (int k=0; k<numThreads; k++) {
			final int offset = k*partsPerThr;
			Thread evictor = new Thread(() -> {
				while (true) {
					for (int i = offset; i < offset+partsPerThr; i++) {
						cache[i].evictEntries();
					}

					int perSeg = totalSegments / numPartitions;

					double max = 0;
					for (int i = 0; i < numPartitions; i++) {
						double size = cache[i].size() * 100.0 / perSeg;
						if (max < size)
							max = size;
					}

					try {
						Thread.sleep(500);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}

					/*if (max > 90) {
						LockSupport.parkNanos(1000000);
					} else {
						try {
							Thread.sleep(100);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}*/
				}
			});
			evictor.setName("CacheEvictor");
			evictor.setDaemon(true);
			evictor.start();
		}
	}

	// fixme: For debugging only
	public void runStatsCollector() {
		Thread collector = new Thread(() -> {
			SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd 'at' HH:mm:ss:SSS");
			Date date = new Date(System.currentTimeMillis());
			System.out.println("Current time: " + formatter.format(date));

			//Accumulator accm = new Accumulator(10000);
			//ApproximateClock clock = ApproximateClock.instance();
			//long startT = clock.currentTime();

			int lsCap = conf.maxBlocksPerLocalDevice * conf.numLocalDevices;
			String outFile = "/home/sm3rizvi/kawkab/experiments/logs/cache-"+conf.thisNodeID+".log";
			File file = new File(outFile).getParentFile();
			if (!file.exists()) {
				file.mkdirs();
			}

			GlobalStoreManager gsm = GlobalStoreManager.instance();

			try (BufferedWriter writer = new BufferedWriter(new FileWriter(outFile));) {
				writer.write(formatter.format(date) + "\n");
				writer.write("# cache occupancy, local store occupancy\n");

				int n = 0;
				while (working) {
					long size = 0;
					for (int i = 0; i < numPartitions; i++) {
						size += cache[i].size();
					}

					double cacheOcc = size * 100.0 / totalSegments;
					double lsOcc = localStore.size() * 100.0 / lsCap;
					double canEvict = localStore.canEvict() * 100.0 / lsCap;
					double gsQlen = gsm.qlen() * 100.0 / lsCap;

					//accm.put((int) ((clock.currentTime() - startT) / 1000.0), occupancy);


					System.out.println(String.format("%d: %.2f, %.2f, %.2f, %.2f\n",++n, cacheOcc, lsOcc, canEvict, gsQlen));
					writer.write(String.format("%.2f, %.2f, %.2f, %.2f\n",cacheOcc, lsOcc, canEvict, gsQlen));

					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}

				writer.write("\n");
				writer.flush();
			} catch (IOException e) {
				e.printStackTrace();
			}
		});
		collector.setName("CacheLogger");
		collector.setDaemon(true);
		collector.start();
	}
}
