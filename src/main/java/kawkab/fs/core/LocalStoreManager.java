package kawkab.fs.core;

import kawkab.fs.commons.Commons;
import kawkab.fs.commons.Configuration;
import kawkab.fs.core.exceptions.FileNotExistException;
import kawkab.fs.core.exceptions.KawkabException;
import kawkab.fs.core.exceptions.OutOfDiskSpaceException;
import kawkab.fs.utils.AccumulatorMap;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Lock;

public final class LocalStoreManager implements SyncCompleteListener {
	private static final int maxBlocks = Configuration.instance().maxBlocksPerLocalDevice; // Number of blocks that can be created locally
	//private static final int numWorkers = Configuration.instance().numLocalDevices; // Number of worker threads and number of reqsQs
	private static final int numDevices = Configuration.instance().numLocalDevices;

	private GlobalStoreManager globalProc;
	private static LocalStoreManager instance;
	
	private TransferQueue<Block> storeQs[]; // Buffer to queue block store requests
	private Thread[] workers;                   // Pool of worker threads that store blocks locally
	private FileChannels[] fileChannels;
	private volatile boolean working = true;
	private final FileLocks fileLocks;

	//private AccumulatorMap readRateLog;
	//private AccumulatorMap[] writeRateLog;

	private ApproximateClock clock = ApproximateClock.instance();
	private final static long START_TIME = System.currentTimeMillis();

	//private LocalStoreCache lc;
	//private final LocalStoreDB storedFilesMap;        // Contains the paths and IDs of the blocks that are currently stored locally
	//private final Semaphore storePermits; // To limit the number of files in the local storage
	private final LSDevice[] devices;

	private class LSDevice {
		private final LocalStoreDB storedFilesMap;
		private final Semaphore storePermits;
		private final LocalStoreCache lscache;
		private final int id;
		private final int maxBlocks;

		private LSDevice(int id, int maxBlocks) {
			this.id = id;
			this.maxBlocks = maxBlocks;
			storedFilesMap = new LocalStoreDB(id, maxBlocks);
			storePermits = new Semaphore(maxBlocks);
			lscache = new LocalStoreCache(id, maxBlocks, storedFilesMap, storePermits);

			int inLocalSystem = storedFilesMap.size();
			assert inLocalSystem <= maxBlocks;
			try {
				storePermits.acquire(inLocalSystem);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public synchronized static LocalStoreManager instance() {
		if (instance == null) {
			instance = new LocalStoreManager();
		}
		
		return instance;
	}
	
	private LocalStoreManager() {
		//workers = Executors.newFixedThreadPool(numWorkers);
		globalProc = GlobalStoreManager.instance();
		fileLocks = FileLocks.instance();

		devices = new LSDevice[numDevices];
		for (int i = 0; i< devices.length; i++) {
			devices[i] = new LSDevice(i, maxBlocks);
		}

		/*storedFilesMap = new LocalStoreDB(maxBlocks);
		storePermits = new Semaphore(maxBlocks);
		lc = new LocalStoreCache(maxBlocks, storedFilesMap, storePermits);
		
		int inLocalSystem = storedFilesMap.size();
		assert inLocalSystem <= maxBlocks;
		try {
			storePermits.acquire(inLocalSystem);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}*/

		//syncWaitMutex = new Object();

		System.out.println("Initializing local store manager. Workers = " + numDevices);
		
		startWorkers();
	}
	
	/**
	 * Start the workers that store blocks in the local store
	 * 
	 * We are not using ExecutorService because we want the same worker to store the same block. We want to assign
	 * work based on the blocks, which is not easily achievable using ExecutorService.
	 */
	private void startWorkers() {
		//readRateLog = new AccumulatorMap(3000);
		//writeRateLog = new AccumulatorMap[numWorkers];

		/*for (int i=0; i<numWorkers; i++) {
			writeRateLog[i] = new AccumulatorMap(3000);
		}*/

		storeQs = new TransferQueue[numDevices];
		for(int i=0; i<numDevices; i++) {
			storeQs[i] = new TransferQueue<>("LSM-TrnsfrQ-"+i);
		}
		
		fileChannels = new FileChannels[numDevices];
		for (int i=0; i<fileChannels.length; i++) {
			fileChannels[i] = new FileChannels("LSM-Chnls-"+i);
		}
		
		workers = new Thread[numDevices];
		for (int i=0; i<workers.length; i++) {
			final int workerID = i;
			workers[i] = new Thread("LocalStoreThread-"+i) {
				public void run() {
					runStoreWorker(workerID);
				}
			};
			
			workers[i].start();
		}
	}

	/**
	 * The workers poll the same queue
	 */
	private void runStoreWorker(int workerID) {
		TransferQueue<Block> reqs = storeQs[workerID];
		FileChannels channels = fileChannels[workerID];
		while(working) {
			Block block = reqs.poll();
			if (block == null) {
				try {
					Thread.sleep(3); //1ms is arbitrary
				} catch (InterruptedException e1) {}
				continue;
			}

			try {
				int synced = processStoreRequest(block, channels);
				//int elapsed = (int)(clock.currentTime() - START_TIME)/1000;
				//writeRateLog[workerID].put(elapsed, synced);
			} catch (KawkabException e) {
				//e.printStackTrace();
				System.out.printf("OOS ");
			}
		}

		// Perform the remaining tasks in the queue
		Block block;
		while( (block = reqs.poll()) != null) {
			try {
				processStoreRequest(block, channels);
			} catch (KawkabException e) {
				e.printStackTrace();
			}
		}

		System.out.println("Closing thread: " + Thread.currentThread().getName());
	}

	/**
	 * This a non-blocking function. The block is added in a queue to be stored locally. Only the dirty bytes
	 * are copied to the local store. If the block is needed to be stored globally, the block is added in the globalStore's
	 * queue after local storage is completed.
	 *
	 * Repeated calls for the same block are coalesced by the block itself. See block.appendOffsetInBlock() function.
	 * If the block is already in the queue, it is not added again. This works because (1) the worker checks that if
	 * the dirty count is non-zero, it tries to add the block again in the queue, (2) the thread that has updated
	 * the block first increments the dirty count and then tries to add in the queue. So the race between the worker
	 * thread and the block-writer thread always result in adding at least one more job in the queue.
	 *
	 * It may happen that the block is added in the queue by the block-writer, but the competing worker has finished
	 * syncing all the dirty bytes. In this case, there will be an extra job in the queue. However, the block will not
	 * by updated redundantly because the worker checks the dirty count before performing the store operation. If the
	 * dirty count is zero, it ignores the job.
	 *
	 * @param block The block to store locally, and potentially globally
	 * @throws KawkabException if the localStore has already received the stop signal for shutting down.
	 * @throws NullPointerException
	 */
	public void store(Block block) {
		// WARNING! This functions works correctly in combination with the processStoreRequest(block) function only if the
		// same block is assigned always to the same worker thread.

		/*if (!working) {
			throw new KawkabException("LocalProcessor has already received stop signal.");
		}*/

		//Load balance between workers, but assign same worker to the same block.
		int queueNum = Math.abs(block.id().perBlockTypeKey()) % numDevices; //TODO: convert hashcode to a fixed computed integer or int based key

		storeQs[queueNum].add(block);
	}

	/**
	 * This function in combination with the store(block) function works correctly for concurrent (a) single worker per block
	 * that syncs the block, and (b) multiple writers that modify/append the block (inodeBlocks are modified concurrently
	 * by many threads, dataSegments are only appended). The writers can concurrently try to add the block in the queue.
	 * If the block is already in the queue, the block is not added again in the queue. This is done through getAndSet(true) in the
	 * block.markInLocalQueue() function.
	 *
	 * @param block
	 * @throws KawkabException
	 */
	private int processStoreRequest(Block block, FileChannels channels) throws KawkabException {
		int syncedCnt = 0;

		if (block.id() == null)
			return 0;

		//System.out.printf("[LSM] Store block: %s\n",block.id());

		// WARNING! This functions works correctly in combination with the store(block) function only if the same block
		// is always assigned to the same worker thread.

		// See the GlobalStoreManager.storeToGlobal(Task) function for an example run where dirtyCnt can be zero.

		// We must clear the dirty bit before syncing the block to the local store. Otherwise, the race with the
		// block writer will not be safe.
		//if (!block.getAndClearLocalDirty()) {
		int dirtyCnt = block.localDirtyCount();
		if (dirtyCnt == 0) {
			block.notifyLocalSyncComplete();
			return 0;
		}

		BlockID bid = block.id();
		Lock lock = fileLocks.grabLock(bid); // To prevent concurrent read from the GlobalStoreManager
		FileChannel channel = null;
		try {
			lock.lock();

			while(dirtyCnt > 0) {

				channel = channels.acquireChannel(bid);

				assert channel.isOpen();

				syncedCnt += block.storeTo(channel);

				dirtyCnt = block.decAndGetLocalDirty(dirtyCnt); //DirtyCnt is counting the number of updates, not the number of dirty bytes.

				if (bid.type() != BlockID.BlockType.DATA_SEGMENT)
					break;
			}

			//channel.force(true);

			//block.markGlobalDirty();
		} catch (IOException e) {
			//System.out.printf("[LSM] Unable to store data for ID: %s, dirty bytes = %d\n", bid, block.decAndGetLocalDirty(0));
			System.out.print("S ");
			//FIXME: What should we do here? Should we return?
			throw new KawkabException(e);
		} catch(Exception | AssertionError e) { //FIXME: For debugging purposes.
			e.printStackTrace();
		} finally {
			if (channel != null) {
				channels.releaseFileChannel(bid);
			}

			lock.unlock();
		}

		//System.out.println("dirtyCnt=0, skipping submittingToGlobal: " + block.id());
		//updateLocalDirty(block, dirtyCnt);
		//block.subtractAndGetLocalDirty(dirtyCnt);

		// We clear the inLocalQueue flag when the global store has finished uploading. This is to mutually exclude updating
		// the local file while concurrently uploading to the global store. If this happens, S3 API throws an exception
		// that the MD5 hash of the file has changed. Now we update this flag in notifyStoreComplete() function.

		//if (block.subtractAndGetLocalDirty(0) > 0) {
		if (block.decAndGetLocalDirty(0) > 0) {
			assert block.id().equals(bid); //If this happens, the same block is being used by multiple appenders. May be the cache is full.

			if (bid.type() != BlockID.BlockType.INODES_BLOCK)
				store(block);
		} else {
			//The race b/w checking the localDirty() and the writer marking it localDirty() is safe. The localStore will
			// eventually get the block.
			block.notifyLocalSyncComplete();

			if (block.shouldStoreGlobally()) { // If this block is the last data segment or an ibmap or an inodesBlock
				globalProc.store(bid, this); // Add the block in the queue to be transferred to the globalStore
			}
		}

		//notifyLocalSynced();

		return syncedCnt;
	}

	/*private long updateLocalDirty(Block block, long dirtyCount) {
		block.clearInLocalQueue();	// We have to do it so that either the current executing 
									// thread or the appender can add the block in the local queue
		return block.subtractAndGetLocalDirty(dirtyCount);
	}*/
	
	@Override
	public void notifyGlobalStoreComplete(BlockID blockID, boolean successful) throws KawkabException {
		// Called by the global store manager when it is done with storing the block in the global store
		
		/* if not successful, add in some queue to retry later  */
		
		/*if (block.isLocalDirty()) {// We have to check the count again. We cannot simply call localDirtyCount()
							// because we need to notify any thread waiting for the block to be removed 
							// from the local queue.
							// Otherwise, a block can be evicted from the cache while the block is still being synced to the local store.
			//System.out.println("[LSM] Local dirty. Adding again: " + block.id() + ", cnt="+dirtyCount);
			store(block);
			return;
		} else {
			// System.out.println("[LSM] Local NOT dirty. Skipping: " + block.id() + ", cnt="+dirtyCount);
			block.notifyLocalSyncComplete();
		}*/

		if (!successful) {
			System.out.println("[LS] Store to global failed for: " + blockID);
			globalProc.store(blockID, this); //FIXME: This doesn't seem to be the right approach
			return;
		}
		
		//System.out.println("[LSM] Finished storing to global: " + block.id());

		/*if (!block.isInCache() && block.evictLocallyOnMemoryEviction()) {
			// Block is not cached. Therefore, the cache will not delete the block.
			// The block is in the local store and it can be deleted.
			// Therefore, mark that the block can be evicted from the local store.
			evictFromLocal(block);
		}*/

		if (blockID.type() == BlockID.BlockType.DATA_SEGMENT || blockID.type() == BlockID.BlockType.INDEX_BLOCK) {
			evictFromLocal(blockID);
		}
	}
	
	//public void notifyEvictedFromCache(Block block) throws KawkabException {
		//block.unsetInCache();
		
		//if (block.globalDirtyCount() == 0 && block.evictLocallyOnMemoryEviction()) {
		/*if (block.evictLocallyOnMemoryEviction()) {
			evictFromLocal(block);
			return;
		}*/

		//TODO: Add in canBeEvicted list. Also, remove from the canBeEvicted list if the block becomes dirty again
	//}
	
	public void evictFromLocal(BlockID id) {
		/*if (id.onPrimaryNode())
			return;
		
		//System.out.println("[LSM] Evict locally: " + id);
		
		if (storedFilesMap.removeEntry(id) == null) {
			return;
		}
		
		File file = new File(id.localPath());
		if (!file.delete()) {
			throw new KawkabException("[LSM] Unable to delete file: " + file.getAbsolutePath());
		}
		
		//block.unsetInLocalStore();
		
		//int mapSize = storedFilesMap.size();
		//int permits = storePermits.availablePermits();
		//System.out.println("\t\t\t\t\t\t Evict: Permits: " + permits + ", map: " + mapSize);
		
		storePermits.release();*/
		//leq.evict(id);

		LSDevice dev = devices[Math.abs(id.perBlockTypeKey()) % numDevices];
		dev.lscache.evict(id);
		//lc.evict(id);
	}
	
	/**
	 * Creates a new file in the underlying file system.
	 * 
	 * Multiple writers can call this function concurrently. However, only one writer per block should be allowed to call this function.
	 * The function assumes that the caller prevents multiple writers from creating the same new block.
	 */
	public void createBlock(BlockID blockID) throws IOException, OutOfDiskSpaceException {
		LSDevice dev = devices[Math.abs(blockID.perBlockTypeKey()) % numDevices];

		if (dev.storePermits.availablePermits() <= 64) { //This number should be more than the number of worker threads.
			dev.lscache.makeSpace();
		}

		if (!dev.storePermits.tryAcquire()) { // This provides an upper limit on the number of blocks that can be created locally.
			throw new OutOfDiskSpaceException (
					String.format("[LSM] Local store %d is full. Available store permits = %d, local store map size = %d, configured capacity = %d, time=%s\n",
							dev.id, dev.storePermits.availablePermits(), dev.storedFilesMap.size(), dev.maxBlocks, Commons.currentTime()));
		}
		File file = new File(blockID.localPath());
		File parent = file.getParentFile();
		if (!parent.exists()){
			parent.mkdirs();
		}
		
		if (!file.createNewFile()) {
			dev.storePermits.release();
			throw new IOException("Unable to create the file: " + blockID.localPath());
		}
		
		dev.storedFilesMap.put(blockID);
	}
	
	/**
	 * This is a blocking function. The block is loaded from the local store if the block is available in it. Otherwise,
	 * the block is loaded from the global store. The function blocks until it is loaded from the local or the global
	 * store.
	 * 
	 * @param block The block to load from the local/global store.
	 * @throws FileNotExistException If the block 
	 * @throws KawkabException
	 * @return false if the block is not available in the local store. Returns true if the block is loaded successfully
	 * from the local store.
	 */
	public boolean load(Block block) throws FileNotExistException, IOException {
		LSDevice dev = devices[Math.abs(block.id().perBlockTypeKey()) % numDevices];
		BlockID id = block.id();
		System.out.println("[LSM] Load block: " + id + " from device " + dev.id);


		if (!dev.storedFilesMap.exists(id)) {
			System.out.println("[LSM] Block is not available locally: " + id);
			return false;
		}

		dev.lscache.touch(id);
		
		int loaded = block.loadFromFile();

		//int elapsed = (int)(clock.currentTime() - START_TIME)/1000;
		//readRateLog.put(elapsed, loaded);

		return true;
	}
	
	public void shutdown() {
		System.out.println("[LSM] Closing LocalStoreManager...");
		
		if (workers == null)
			return;
		
		working = false;

		//System.out.println("[LSM] Saving read/write rates before checking for any remaining jobs.");
		//saveDataRates(writeRateLog, true);
		//saveDataRates(new AccumulatorMap[]{readRateLog}, false);
		
		for (int i=0; i<numDevices; i++) {
			try {
				workers[i].join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		System.out.printf("[LSM] LocalStore start time ..., %s\n", new SimpleDateFormat("HH:mm:ss.SSS").format(new Date(START_TIME)));

		System.out.println("[LSM] Stopped workers, checking for any remaining jobs.");
		for (int i=0; i<storeQs.length; i++) {
			Block block;
			while( (block = storeQs[i].poll()) != null) {
				try {
					processStoreRequest(block, fileChannels[i]);
				} catch (KawkabException e) {
					e.printStackTrace();
				}
			}
		}
		
		for (int i=0; i<storeQs.length; i++) {
			assert storeQs[i].size() == 0;
			storeQs[i].shutdown();
		}
		
		globalProc.shutdown();

		for (LSDevice dev : devices) {
			dev.storedFilesMap.shutdown();
		}
		
		for (int i=0; i<fileChannels.length; i++) {
			fileChannels[i].shutdown();
		}

		System.out.println("[LSM] Closed LocalStoreManager");
	}

	public boolean exists(BlockID id) { // FIXME: This function needs to be synchronized with the createNewBlock and evictFromLocal functions
		if (!working)
			return false;

		LSDevice dev = devices[Math.abs(id.perBlockTypeKey()) % numDevices];
		return dev.storedFilesMap.exists(id);
	}

	/*private void notifyLocalSynced() {
		synchronized (syncWaitMutex) {
			syncWaitMutex.notify();
		}
	}

	public void syncWait() throws InterruptedException{
		synchronized (syncWaitMutex) {
			syncWaitMutex.wait();
		}
	}*/

	public int size() {
		int cnt = 0;
		for (LSDevice dev : devices) {
			cnt += dev.storedFilesMap.size();
		}
		return cnt;
		//return storedFilesMap.size();
	}

	public int permits() {
		//return storePermits.availablePermits();
		int cnt = 0;
		for (LSDevice dev : devices) {
			cnt += dev.storePermits.availablePermits();
		}
		return cnt;
	}

	public int canEvict() {
		int cnt = 0;
		for (LSDevice dev : devices) {
			cnt += dev.lscache.size();
		}
		return cnt;
		//return lc.size();
	}

	public synchronized void saveDataRates(AccumulatorMap[] accms, boolean isWritesMap) {
		try {
			String uldl = isWritesMap ? "lswrites" : "lsreads";

			String outFolder = System.getProperty("outFolder", "/home/sm3rizvi/kawkab/experiments/logs");
			String outFile = String.format("%s/server-%d-%s.txt", outFolder, Configuration.instance().thisNodeID, uldl);

			for (AccumulatorMap am : accms) {
				am.exportJson(outFile);
			}
		}catch (Exception | AssertionError e) {
			e.printStackTrace();
		}
	}
}
