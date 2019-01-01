package kawkab.fs.core;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import kawkab.fs.commons.Constants;
import kawkab.fs.core.exceptions.FileNotExistException;
import kawkab.fs.core.exceptions.KawkabException;

public class GlobalStoreManager {
	private static final Object initLock = new Object();
	private GlobalBackend[] backends; // To interact with the global store
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
		backends = new GlobalBackend[numWorkers];
		for(int i=0; i<numWorkers; i++) {
			backends[i] = new S3Backend();
		}
		
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
			workers[i] = new Thread("GlobalStoreWorker-"+workerID) {
				public void run() {
					runStoreWorker(storeQs[workerID], backends[workerID]);
				}
			};
			
			workers[i].start();
		}
	}
	
	/**
	 * This is a blocking function.
	 * .
	 * @param block Destination block in which data will be loaded.
	 * @throws FileNotExistException
	 * @throws KawkabException
	 */
	public void load(Block block) throws FileNotExistException, KawkabException {
		//TODO: Limit the number of load requests, probably using semaphore
		//TODO: Make it a blocking function and use a threadpool for the load requests
		
		backends[0].loadFromGlobal(block);
	}
	
	public void store(Block block, SyncCompleteListener listener) throws KawkabException {
		/*if (!working) {
			throw new KawkabException("GlobalStoreManager has already received stop signal.");
		}*/
		
		if (block.markInGlobalQueue()) { // The block is already in the queue or being processed
			//System.out.println("\t\t[GSM] Skip: " + block.id());
			return;
		}
		
		//System.out.println("\t\t\t[GSM] Enque: " + block.id());
		
		//Load balance between workers, but assign same worker to the same block.
		int queueNum = Math.abs(block.id().uniqueKey().hashCode()) % numWorkers; //TODO: convert hashcode to a fixed computed integer or int based key
		
		storeQs[queueNum].add(new Task(block, listener));
	}
	
	/**
	 * The workers poll the same queue 
	 */
	private void runStoreWorker(LinkedBlockingQueue<Task> reqs, GlobalBackend backend) {
		while(true) {
			Task task = null;
			try {
				task = reqs.poll(3, TimeUnit.SECONDS);
			} catch (InterruptedException e1) {
				if (!working) {
					break;
				}
			}
		
			if (task == null) {
				if (!working)
					break;
				continue;
			}
			
			try {
				storeToGlobal(task, backend);
			} catch (KawkabException e) {
				e.printStackTrace();
			}
		}
		
		// Perform the remaining tasks in the queue
		Task task = null;
		while( (task = reqs.poll()) != null) {
			try {
				storeToGlobal(task, backend);
			} catch (KawkabException e) {
				e.printStackTrace();
			}
		}
	}
	
	private void storeToGlobal(Task task, GlobalBackend backend) throws KawkabException {
		//System.out.println("[GSM] Storing: " + task.block.id());
		
		Block block = task.block;
		int count = block.globalDirtyCount();
		
		//assert count > 0; // count must be greater than zero because the block is added in the queue only if its dirty count is non-zero.
		
		// It may happen that the count is equal to zero. For example, assume dirty count is 1. The worker thread preempts 
		// just before getting the current dirty count. The appender modifies the block and increments the dirty count to two.
		// The worker preempts just before executing the line "block.markInGlobalQueue()" in this class's store() function.
		// The worker thread resumes, get the dirty count to be 2, stores to the global store, clears the dirty count,
		// which becomes zero. The worker clears the inGlobalQueue flag, notifiesStoreComplete() and returns. Now the
		// dirty count is zero. The appender wakes up, checks block.markInGlobalQueue, which turns out to be previously
		// false. Therefore, the appender adds the block again in the queue, even though now the dirty count is zero.
		// Therefore, we can reach this line in the code when the dirty count is zero.
		
		boolean successful = true;
		if (count > 0) {
			try {
				backend.storeToGlobal(block);
			} catch (KawkabException e) {
				e.printStackTrace();
				successful = false;
			}
		}
		
		block.clearInGlobalQueue(); // We must get the current dirty count after clearing the inQueue flag because a
		                               // concurrent writer may want to add the block in the queue. If the concurrent
									   // writer fails to add in the queue, then this worker should add the block in the
									   // if the dirty count is non-zero.
		
		count = block.decAndGetGlobalDirty(count);
		if (count > 0) {
			//System.out.println("[GSM] Global bit dirty, resubmitting block: " + block.id() + ", cnt="+count);
			store(block, task.listener);
			return;
		}
		
		try {
			task.listener.notifyGlobalStoreComplete(block, successful);
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
				//workers[i].interrupt();
				workers[i].join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		for (int i=0; i<storeQs.length; i++) {
			Task task = null;
			while( (task = storeQs[i].poll()) != null) {
				try {
					storeToGlobal(task, backends[0]);
				} catch (KawkabException e) {
					e.printStackTrace();
				}
			}
		}
		
		for(GlobalBackend backend : backends) {
			backend.shutdown();
		}
		
		for (int i=0; i<storeQs.length; i++) {
			assert storeQs[i].size() == 0;
		}
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
