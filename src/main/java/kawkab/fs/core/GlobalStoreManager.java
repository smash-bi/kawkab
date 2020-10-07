package kawkab.fs.core;

import kawkab.fs.commons.Configuration;
import kawkab.fs.core.exceptions.FileNotExistException;
import kawkab.fs.core.exceptions.KawkabException;
import kawkab.fs.utils.AccumulatorMap;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class GlobalStoreManager {
	private static final Object initLock = new Object();
	//private GlobalBackend[] storeBackends; // To store blocks to the global store
	private GlobalBackend[] loadBackends; // To load a block from the global store
	//private LinkedBlockingQueue<Task> storeQs[]; // Buffer to queue block store requests
	private LinkedBlockingQueue<Task> storeQ; // Buffer to queue block store requests
	private Thread[] workers;                   // Pool of worker threads that store blocks globally
	private final int numWorkers = Configuration.instance().numWorkersStoreToGlobal; // Number of worker threads and number of reqsQs
	private volatile boolean working = true;    // To stop accepting new requests after working is false

	private final int numBackends = Configuration.instance().minioServers.length;
	
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
		//loadWorker = new S3Backend();
		//storeBackends = new GlobalBackend[numBackends];
		loadBackends = new GlobalBackend[numBackends];
		for(int i=0; i<numBackends; i++) {
			GlobalBackend gb = new S3Backend(i);
			//storeBackends[i] = gb;
			loadBackends[i] = gb;
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
		storeQ = new LinkedBlockingQueue<>();
		/*storeQs = new LinkedBlockingQueue[numWorkers];
		for(int i=0; i<numWorkers; i++) {
			//storeQs[i] = new LinkedBlockingQueue<>();
			storeQs[i] = q;
		}*/

		workers = new Thread[numWorkers];
		for (int i=0; i<workers.length; i++) {
			final int workerID = i;
			workers[i] = new Thread("GlobalStoreWorker-"+workerID) {
				public void run() {
					runStoreWorker(storeQ);
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
	public void bulkLoad(Block block, int offset, int length) throws FileNotExistException, IOException {
		//TODO: Limit the number of load requests, probably using semaphore
		//TODO: Make it a blocking function and use a threadpool for the load requests

		int backend = Math.abs(block.id().perBlockTypeKey()) % loadBackends.length;

		synchronized(loadBackends[backend]) {
			loadBackends[backend].loadFromGlobal(block, offset, length);
		}
	}

	public void bulkLoad(BlockLoader bl) throws FileNotExistException, IOException {
		int backend = Math.abs(bl.perBlockTypeKey()) % loadBackends.length;

		synchronized(loadBackends[backend]) {
			loadBackends[backend].bulkLoadFromGlobal(bl);
		}
	}
	
	public void store(BlockID blockID, SyncCompleteListener listener) throws KawkabException {
		/*if (!working) {
			throw new KawkabException("GlobalStoreManager has already received stop signal.");
		}*/
		
		/*if (block.markInGlobalQueue()) { // The block is already in the queue or being processed
			//System.out.println("\t\t[GSM] Skip: " + block.id());
			return;
		}*/
		
		//System.out.println("\t\t\t[GSM] Enque: " + block.id());
		
		//Load balance between workers, but assign same worker to the same block.
		//int queueNum = Math.abs(blockID.perBlockTypeKey()) % numWorkers; //TODO: convert hashcode to a fixed computed integer or int based key

		if (blockID.type == BlockID.BlockType.INDEX_BLOCK)
			System.out.println("[GSM] Store to global: " + blockID.localPath());
		
		//storeQs[queueNum].add(new Task(blockID, listener));
		storeQ.add(new Task(blockID, listener));
	}
	
	/**
	 * The workers poll the same queue 
	 */
	private void runStoreWorker(LinkedBlockingQueue<Task> reqs) {
		//LatHistogram storeLog = new LatHistogram(TimeUnit.MILLISECONDS, "GS upload", 100, 10000);

		final Configuration conf = Configuration.instance();
		ByteBuffer buffer = ByteBuffer.allocateDirect((Math.max(conf.dataBlockSizeBytes, conf.inodesBlockSizeBytes)));
		GlobalBackend[] storeBackends = new GlobalBackend[numBackends];
		for(int i=0; i<numBackends; i++) {
			storeBackends[i] = new S3Backend(i);
		}

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

			//storeLog.start();
			try {
				storeToGlobal(task, storeBackends, buffer);
			} catch (KawkabException e) {
				e.printStackTrace();
			}
			//int ms = storeLog.end();
			//System.out.printf("Upload (ms): %d, qlen=%d\n", ms, reqs.size());
		}

		saveDataRates(storeBackends, true);
		
		// Perform the remaining tasks in the queue
		Task task = null;
		while( (task = reqs.poll()) != null) {
			try {
				storeToGlobal(task, storeBackends, buffer);
			} catch (KawkabException e) {
				e.printStackTrace();
				break;
			}
		}

		for (int i=0; i<storeBackends.length; i++) {
			storeBackends[i].shutdown();
		}
	}
	
	private void storeToGlobal(Task task, GlobalBackend[] backends, ByteBuffer buffer) throws KawkabException {
		//System.out.println("[GSM] Storing: " + task.block.id());
		
		//Block block = task.blockID;
		
		//block.clearInGlobalQueue(); // We must get the current dirty count after clearing the inQueue flag because a
		// concurrent writer may want to add the block in the queue. If the concurrent
		// writer fails to add in the queue, then this worker should add the block in the
		// if the dirty count is non-zero.
		
		//int count = block.globalDirtyCount();
		
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

		/*if (count > 0) {
			try {
				backend.storeToGlobal(block);
			} catch (KawkabException e) {
				e.printStackTrace();
				successful = false;
			}
		}
		
		count = block.decAndGetGlobalDirty(count);
		if (count > 0) {
			//System.out.println("[GSM] Global bit dirty, resubmitting block: " + block.id() + ", cnt="+count);
			store(block, task.listener);
			return;
		}*/

		int backend = Math.abs(task.blockID.perBlockTypeKey()) % backends.length;

		backends[backend].storeToGlobal(task.blockID, buffer);

		//backend.storeToGlobal(task.blockID, buffer);

		try {
			task.listener.notifyGlobalStoreComplete(task.blockID, successful);
		} catch (KawkabException e) {
			e.printStackTrace();
		}
	}
	
	public void shutdown() {
		System.out.println("Closing global store manager");

		working = false;

		saveDataRates(loadBackends, false);

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

		/*Configuration conf = Configuration.instance();
		ByteBuffer buffer = ByteBuffer.allocateDirect((Math.max(conf.dataBlockSizeBytes, conf.inodesBlockSizeBytes)));
		for (int i=0; i<storeQs.length; i++) {
			Task task = null;
			while( (task = storeQs[i].poll()) != null) {
				//int num = Math.abs(task.blockID.perBlockTypeKey()) % numWorkers;
				try {
					storeToGlobal(task, storeBackends, buffer);
				} catch (KawkabException e) {
					e.printStackTrace();
				}
			}
		}
		
		for(GlobalBackend backend : storeBackends) {
			backend.shutdown();
		}*/

		for(GlobalBackend backend : loadBackends) {
			backend.shutdown();
		}

		/*for (int i=0; i<storeQs.length; i++) {
			assert storeQs[i].size() == 0;
		}*/

		if (storeQ.size() != 0) {
			System.out.println("[GSM] Error: storeQ is not empty: " + storeQ.size());
		}
		
		System.out.println("Closed GlobalStoreManager");
	}
	
	private class Task {
		final SyncCompleteListener listener;
		final BlockID blockID;
		Task(BlockID blockID, SyncCompleteListener listener){
			this.blockID = blockID;
			this.listener = listener;
		}
	}

	public int qlen() {
		/*int size = 0;
		for (int i=0; i<storeQs.length; i++) {
			size += storeQs[i].size();
		}

		return size;*/
		return storeQ.size();
	}

	private synchronized void saveDataRates(GlobalBackend[] backends, boolean isUpload) {
		try {
			String uldl = isUpload ? "upload" : "download";

			String outFolder = System.getProperty("outFolder", "/home/sm3rizvi/kawkab/experiments/logs");
			String outFile = String.format("%s/server-%d-%s.txt", outFolder, Configuration.instance().thisNodeID, uldl);

			for (GlobalBackend be : backends) {
				AccumulatorMap am;
				if (isUpload)
					am = be.getUploadStats();
				else
					am = be.getDownloadStats();

				am.exportJson(outFile);
			}
		}catch (Exception | AssertionError e) {
			e.printStackTrace();
		}
	}
}
