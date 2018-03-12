package kawkab.fs.core;

import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import kawkab.fs.commons.Constants;
import kawkab.fs.core.exceptions.FileNotExistException;
import kawkab.fs.core.exceptions.KawkabException;

public class GlobalStoreManager {
	private static final Object initLock = new Object();
	private GlobalBackend backend; // To interact with the global store
	private LinkedBlockingQueue<Task> storeQs[]; // Buffer to queue block store requests
	private Thread[] workers;                   // Pool of worker threads that store blocks globally
	private final int numWorkers;               // Number of worker threads and number of reqsQs
	private volatile boolean working = true;    // To stop accepting new requests after working is false
	
	private static GlobalStoreManager instance;
	
	public static GlobalStoreManager instance() {
		if (instance == null) {
			synchronized(initLock) {
				if (instance == null)
					instance = new GlobalStoreManager();
			}
		}
		
		return instance;
	}
	
	private GlobalStoreManager() {
		numWorkers = Constants.numWorkersStoreToGlobal;
		backend = new S3Backend();
		
		startWorkers();
	}
	
	/**
	 * Start the workers that store blocks in the global store
	 * 
	 * We are not using ExecutorService because we want the same worker to store the same block. We want to assign
	 * work based on the blocks, which is not easily achievable using ExecutorService.
	 */
	private void startWorkers() {
		storeQs = new LinkedBlockingQueue[numWorkers];
		for(int i=0; i<numWorkers; i++) {
			storeQs[i] = new LinkedBlockingQueue<Task>();
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
	 * This is a blocking function.
	 * @param destBlock
	 * @throws FileNotExistException
	 * @throws KawkabException
	 */
	public void load(Block destBlock) throws FileNotExistException, KawkabException {
		//TODO: Limit the number of load requests, probably using semaphore
		
		backend.loadFromGlobal(destBlock);
	}
	
	public void store(Block srcBlock, SyncCompleteListener listener) throws KawkabException {
		if (!working) {
			throw new KawkabException("LocalProcessor has already received stop signal.");
		}
		
		if (srcBlock.markInGlobalQueue()) { // The block is already in the queue or being processed
			//System.out.println("[GSM] Already in queue: " + srcBlock.id());
			return;
		}
		
		//Load balance between workers, but assign same worker to the same block.
		int queueNum = Math.abs(srcBlock.id().key().hashCode()) % numWorkers; //TODO: convert hashcode to a fixed computed integer or int based key
		
		storeQs[queueNum].add(new Task(srcBlock, listener));
	}
	
	/**
	 * The workers poll the same queue 
	 */
	private void runStoreWorker(LinkedBlockingQueue<Task> reqs) {
		while(working) {
			Task task = null;
			try {
				task = reqs.take();
			} catch (InterruptedException e1) {
				if (!working) {
					break;
				}
			}
		
			if (task == null)
				continue;
			
			try {
				storeToGlobal(task);
			} catch (KawkabException e) {
				e.printStackTrace();
			}
		}
		
		// Perform the remaining tasks in the queue
		Task task = null;
		while( (task = reqs.poll()) != null) {
			try {
				storeToGlobal(task);
			} catch (KawkabException e) {
				e.printStackTrace();
			}
		}
	}
	
	private void storeToGlobal(Task task) throws KawkabException {
		Block srcBlock = task.block;
		int count = srcBlock.globalDirtyCount();
		
		assert count > 0; // count must be greater than zero because the block is added in the queue only if its dirty count is non-zero.
		
		boolean successful = true;
		try {
			backend.storeToGlobal(srcBlock);
		} catch (KawkabException e) {
			e.printStackTrace();
			successful = false;
		}
		
		srcBlock.clearInGlobalQueue(); //This must be cleared before clearing the dirt bit. Otherwise, we may miss a block update.
		
		count = srcBlock.clearAndGetGlobalDirty(count);
		if (count > 0) {
			//System.out.println("[GSM] Global bit dirty, resubmitting block: " + srcBlock.id());
			store(srcBlock, task.listener);
			return;
		}
		
		try {
			task.listener.notifyStoreComplete(srcBlock, successful);
		} catch (KawkabException e) {
			e.printStackTrace();
		}
	}
	
	public void shutdown() {
		System.out.println("Closing global store manager");
		
		working = false;
		
		if (workers == null)
			return;
		
		for (int i=0; i<numWorkers; i++) {
			try {
				workers[i].interrupt();
				workers[i].join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		backend.shutdown();
	}
	
	private class Task {
		final SyncCompleteListener listener;
		final Block block;
		Task(Block block, SyncCompleteListener listener){
			this.block = block;
			this.listener = listener;
		}
	}
}
