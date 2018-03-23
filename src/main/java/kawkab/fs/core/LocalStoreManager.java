package kawkab.fs.core;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.SeekableByteChannel;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import kawkab.fs.commons.Constants;
import kawkab.fs.core.exceptions.FileNotExistException;
import kawkab.fs.core.exceptions.KawkabException;

public final class LocalStoreManager implements SyncCompleteListener {
	private static final Object initLock = new Object();
	
	//private ExecutorService workers;
	private GlobalStoreManager globalProc;
	
	private final int maxBlocks = Constants.maxBlocksPerLocalDevice; // Number of blocks that can be created locally
	private final int numWorkers;               // Number of worker threads and number of reqsQs
	private LinkedBlockingQueue<Block> storeQs[]; // Buffer to queue block store requests
	private Thread[] workers;                   // Pool of worker threads that store blocks locally
	private LocalStoreDB storedFilesMap;        // Contains the paths and IDs of the blocks that are currently stored locally
	private Semaphore storePermits;
	private volatile boolean working = true;
	
	private static LocalStoreManager instance;
	
	public static LocalStoreManager instance() throws IOException { 
		if (instance == null) {
			synchronized(initLock) {
				if (instance == null)
					instance = new LocalStoreManager();
			}
		}
		
		return instance;
	}
	
	private LocalStoreManager() throws IOException {
		//workers = Executors.newFixedThreadPool(numWorkers);
		globalProc = GlobalStoreManager.instance();
		numWorkers = Constants.syncThreadsPerDevice;
		
		storedFilesMap = new LocalStoreDB(maxBlocks);
		storePermits = new Semaphore(maxBlocks);
		
		int inLocalSystem = storedFilesMap.size();
		assert inLocalSystem <= maxBlocks;
		try {
			storePermits.acquire(inLocalSystem);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		System.out.println("Initializing local store manager");
		
		startWorkers();
	}
	
	/**
	 * Start the workers that store blocks in the local store
	 * 
	 * We are not using ExecutorService because we want the same worker to store the same block. We want to assign
	 * work based on the blocks, which is not easily achievable using ExecutorService.
	 */
	private void startWorkers() {
		storeQs = new LinkedBlockingQueue[numWorkers];
		for(int i=0; i<numWorkers; i++) {
			storeQs[i] = new LinkedBlockingQueue<Block>();
		}
		
		workers = new Thread[numWorkers];
		for (int i=0; i<workers.length; i++) {
			final int workerID = i;
			workers[i] = new Thread("LocalStoreThread-"+i) {
				public void run() {
					runStoreWorker(storeQs[workerID]);
				}
			};
			
			workers[i].start();
		}
	}
	
	/**
	 * The workers poll the same queue 
	 */
	private void runStoreWorker(LinkedBlockingQueue<Block> reqs) {
		while(true) {
			Block block = null;
			try {
				block = reqs.poll(3, TimeUnit.SECONDS);
			} catch (InterruptedException e1) {
				if (!working) {
					break;
				}
			}
		
			if (block == null) {
				if (!working)
					break;
				continue;
			}
			
			try {
				processStoreRequest(block);
			} catch (KawkabException e) {
				e.printStackTrace();
			}
		}
		
		
		
		// Perform the remaining tasks in the queue
		Block block = null;
		while( (block = reqs.poll()) != null) {
			try {
				processStoreRequest(block);
			} catch (KawkabException e) {
				e.printStackTrace();
			}
		}
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
	public void store(Block block) throws KawkabException {
		// WARNING! This functions works correctly in combination with the processStoreRequest(block) function only if the 
		// same block is assigned always to the same worker thread.
		
		/*if (!working) {
			throw new KawkabException("LocalProcessor has already received stop signal.");
		}*/
		
		if (block.markInLocalQueue()) { //The block is already in the queue, the block should not be added again.
			//System.out.println("[LSM] Skipping: " + block.id());
			return;
		}
		
		//System.out.println("\t[LSM] Enque: " + block.id());
		
		//Load balance between workers, but assign same worker to the same block.
		int queueNum = Math.abs(block.id().key().hashCode()) % numWorkers; //TODO: convert hashcode to a fixed computed integer or int based key
		
		storeQs[queueNum].add(block);
	}
	
	/**
	 * This function in combination with the store(block) function works correctly for concurrent (a) single worker per block
	 * that syncs the block, and (b) multiple writers that modify/append the block (inodeBlocks are modified concurrently
	 * by many threads, dataSegments are only appended). The writers can concurrently try to add the block in the queue.
	 * If the block is already in the queue, the block is not added in the queue. This is done through getAndSet(true) in the
	 * block.markInLocalQueue() function. In the case of a race between the 
	 * 
	 * @param block
	 * @throws KawkabException
	 */
	private void processStoreRequest(Block block) throws KawkabException {
		int dirtyCount = block.localDirtyCount();
		
		// WARNING! This functions works correctly in combination with the store(block) function only if the same block 
		// is assigned always to the same worker thread.
		
		// See the GlobalStoreManager.storeToGlobal(Task) function for an example run where dirtyCount can be zero.
		
		if (dirtyCount > 0) {
			try {
				syncLocally(block);
			} catch (IOException e) {
				e.printStackTrace();
				//FIXME: What should we do here? Should we return?
			}
			
			if (block.shouldStoreGlobally()) {
				globalProc.store(block, this);
			} else {
				block.clearInLocalQueue();
				if (block.clearAndGetLocalDirty(dirtyCount) > 0) {
					store(block);
				}
				
				return;
			}
		}
		
		block.clearAndGetLocalDirty(dirtyCount);
		
		// We clear the inLocalQueue flag when the global store has finished uploading. This is to mutually exclude updating
		// the local file while concurrently uploading to the global store. If this happens, S3 API throws an exception
		// that the MD5 hash of the file has changed.
		
		/*block.clearInLocalQueue(); //This is now done in the notifyStoreComplete function
		if (block.clearAndGetLocalDirty(dirtyCount) > 0) {
			store(block);
		}*/
	}
	
	/**
	 * Creates a new file in the underlying file system.
	 * 
	 * Multiple writers can call this function concurrently. However, only one writer per block should call this function.
	 * The function assumes that the cache prevents multiple writers from creating the same new block.
	 */
	public void createBlock(Block block) throws IOException, InterruptedException {
		
		storePermits.acquire(); // This provides an upper limit on the number of blocks that can be created locally.
		
		File file = new File(block.id().localPath());
		File parent = file.getParentFile();
		if (!parent.exists()){
			parent.mkdirs();
		}
		
		syncLocally(block);
		storedFilesMap.put(block.id());
		block.setInLocalStore(); //Mark the block as locally saved
	}
	
	private void syncLocally(Block block) throws IOException {
		//FIXME: This creates a new file if it does not already exist. We should prevent that in order to make sure that
		//first we create a file and do proper accounting for the file.
		
		//useBlock(); //FIXME: Limit the number of files created in the local storage.
		
		try(RandomAccessFile rwFile = 
                new RandomAccessFile(block.id().localPath(), "rw")) {
			try (SeekableByteChannel channel = rwFile.getChannel()) {
				channel.position(block.appendOffsetInBlock());
				//System.out.println("Store: "+block.id() + ": " + channel.position());
				block.storeTo(channel);
			}
		}
	}
	
	/**
	 * This is a blocking function. The block is loaded from the local store if the block is available in it. Otherwise,
	 * the block is loaded from the global store. The function blocks until it is loaded from the local or the global
	 * store.
	 * 
	 * @param block The block to load from the local/global store.
	 * @throws FileNotExistException If the block 
	 * @throws KawkabException
	 * @return false if the block is not available in the local store. Returns true if the block is loaded successfuly
	 * from the local store.
	 */
	public boolean load(Block block) throws FileNotExistException,KawkabException {
		BlockID id = block.id();
		//System.out.println("[LSM] Load block: " + id.name());
		
		if (!storedFilesMap.exists(id)) {
			System.out.println("[LSM] Block is not available locally: " + id.name());
			return false;
		}
		
		try {
			loadBlock(block);
			block.setInLocalStore();
		} catch (IOException e) {
			throw new KawkabException(e);
		}
		
		return true;
	}
	
	private void loadBlock(Block block) throws IOException {
		try(RandomAccessFile file = 
                new RandomAccessFile(block.id().localPath(), "r")) {
			try (SeekableByteChannel channel = file.getChannel()) {
				channel.position(block.appendOffsetInBlock());
				//System.out.println("Load: "+block.localPath() + ": " + channel.position());
				block.loadFrom(channel);
			}
		}
	}
	
	@Override
	public void notifyStoreComplete(Block block, boolean successful) throws KawkabException {
		/* if not successful, add in some queue to retry later
		 * 
		 * atomically increment dirty count
		 * if dirty count after increment is 2 (dirty count's initial value is 0), this means
		 * the block is not in cache and the block is persisted globally. Therefore, the block can be
		 * deleted from the local store.
		 * 
		*/
		
		block.clearInLocalQueue(); // We have to do it so that either the current executing 
		                           // thread or the appender can add the block in the local queue
		
		if (block.clearAndGetLocalDirty(0) > 0) {	// We have to check the count again. We cannot simply call localDirtyCount() 
													// because we need to notify any thread waiting for the block to be removed 
													// from the local queue.
													// Otherwise, a block can be evicted from the cache while the block is still being synced to the local store.
			//System.out.println("[LSM] Local dirty. Adding again: " + block.id());
			store(block);
			return;
		}
		
		//System.out.println("[LSM] Finished storing to global: " + block.id());
		
		if (!block.isInCache() && block.evictLocallyOnMemoryEviction()) {
			// Block is not cached. Therefore, the cache will not delete the block.
			// The block is in the local store and it can be deleted.
			// Therefore, mark that the block can be evicted from the local store.
			evict(block);
		}
	}
	
	public void notifyEvictedFromCache(Block block) throws KawkabException {
		block.unsetInCache();
		
		if (block.globalDirtyCount() == 0 && block.evictLocallyOnMemoryEviction()) {
			evict(block);
		}
	}
	
	private void evict(Block block) throws KawkabException {
		if (block.id().onPrimaryNode() && !block.isInLocal())
			return;
		
		BlockID id = block.id();
		
		//System.out.println("[LSM] Evict locally: " + id);
		
		if (storedFilesMap.removeEntry(id) == null) { //The cache and the global store race to evict this block.
			return;
		}
		
		File file = new File(id.localPath());
		if (!file.delete()) {
			throw new KawkabException("[LSM] Unable to delete file: " + file.getAbsolutePath());
		}
		
		block.unsetInLocalStore();
		
		//int mapSize = storedFilesMap.size();
		//int permits = storePermits.availablePermits();
		//System.out.println("\t\t\t\t\t\t Evict: Permits: " + permits + ", map: " + mapSize);
		
		storePermits.release();
	}
	
	public void stop() {
		System.out.println("Closing LocalProcessor...");
		
		if (workers == null)
			return;
		
		working = false;
		
		for (int i=0; i<numWorkers; i++) {
			//workers[i].interrupt();
			try {
				//workers[i].interrupt();
				workers[i].join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public boolean exists(BlockID id) { // FIXME: This function needs to be synchronized with the createNewBlock and evict functions
		return storedFilesMap.exists(id);
	}
}
