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
	private volatile boolean working = true;    // To stop accepting new requests after working is false
	private LocalStoreDB storedFilesMap;        // Contains the paths and IDs of the blocks that are currently stored locally
	private Semaphore storePermits;
	
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
		
		storePermits = new Semaphore(maxBlocks);
		storedFilesMap = new LocalStoreDB();
		
		startWorkers();
	}
	
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
	
	private void runStoreWorker(LinkedBlockingQueue<Block> reqs) {
		while(working) {
			Block block = null;
			try {
				block = reqs.poll(1, TimeUnit.SECONDS);
			} catch (InterruptedException e1) {
				if (!working) {
					break;
				}
			}
		
			if (block == null)
				continue;
			
			try {
				processStoreRequest(block);
			} catch (KawkabException e) {
				e.printStackTrace();
			}
		}
		
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
	 * 
	 * @param block The block to store locally, and potentially globally
	 * @throws KawkabException if the localStore has already received the stop signal for shutting down.
	 * @throws NullPointerException
	 */
	public void store(Block block) throws KawkabException {
		if (!working) {
			throw new KawkabException("LocalProcessor has already received stop signal.");
		}
		
		if (block.markInLocalQueue()) { //The block is already in the queue
			System.out.println("[LSM] Block already in the local queue: " + block.id());
			return;
		}
		
		//Load balance between workers, but assign same worker to the same block.
		int queueNum = Math.abs(block.id().key().hashCode()) % numWorkers; //TODO: convert hashcode to a fixed computed integer or int based key
		
		storeQs[queueNum].add(block);
	}
	
	private void processStoreRequest(Block block) throws KawkabException {
		//System.out.println("[LSM] Store block: " + block.id().name());
		
		int dirtyCount = block.localDirtyCount();
		
		// This functions works correctly in combination with the store(block) function only if the same block is always 
		// assigned to the same worker thread.
		
		if (dirtyCount == 0) { 
			block.clearInLocalQueue();
			return;
		}
		
		try {
			storeBlock(block);
			storedFilesMap.put(block.id()); //FIXME: What if an error occurs here. Should we remove the locally stored block?
			
			if (block.shouldStoreGlobally()) {
				globalProc.store(block, this);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		block.clearInLocalQueue();
		
		int cnt = block.clearAndGetLocalDirty(dirtyCount);
		
		//System.out.println("[LSM] Cleared to " + cnt + ", block: " + block.id());
		
		if (cnt > 0) {
			store(block);
		}
	}
	
	public void createBlock(Block block) throws IOException, InterruptedException {
		//System.out.println("[LSM] Create block: " + block.id() + ", available storePermits: " + storePermits.availablePermits());
		storePermits.acquire();
		
		File file = new File(block.id().localPath());
		File parent = file.getParentFile();
		if (!parent.exists()){
			parent.mkdirs();
		}
		
		storeBlock(block);
		storedFilesMap.put(block.id());
		block.setInLocalStore(); //Mark the block as locally saved
	}
	
	private void storeBlock(Block block) throws IOException {
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
		
		if (!block.isInCache()) {
			// Block is not cached. Therefore, the cache will not delete the block.
			// The block is in the local store and it can be deleted.
			// Therefore, mark that the block can be evicted from the local store.
			evict(block);
		}
	}
	
	public void notifyEvictedFromCache(Block block) throws KawkabException {
		System.out.println("[LSM] Evicted from cache.");
		
		if (block.globalDirtyCount() == 0) {
			evict(block);
		}
		
		block.unsetInCache(); // We must set this flag after eviction. Otherwise, the global store thread may race
		                         // with this thread to evict the block, which will result in deleting a non-existing block.
								 // Moreover, it will free an extra storePermit.
	}
	
	private void evict(Block block) throws KawkabException {
		if (!block.isInLocal())
			return;
		
		BlockID id = block.id();
		
		System.out.println("[LSM] Evict: " + id);
		
		storedFilesMap.removeEntry(id);
		
		File file = new File(id.localPath());
		if (!file.delete()) {
			throw new KawkabException("[LSM] Unable to delete file: " + file.getAbsolutePath());
		}
		
		block.unsetInLocalStore();
		storePermits.release();
	}
	
	public void stop() {
		System.out.println("Closing LocalProcessor...");
		
		working = false;
		
		if (workers == null)
			return;
		
		for (int i=0; i<numWorkers; i++) {
			//workers[i].interrupt();
			try {
				workers[i].join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public boolean exists(BlockID id) {
		return storedFilesMap.exists(id);
	}
}
