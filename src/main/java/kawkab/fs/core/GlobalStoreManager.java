package kawkab.fs.core;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import kawkab.fs.commons.Constants;
import kawkab.fs.core.exceptions.FileNotExistException;
import kawkab.fs.core.exceptions.KawkabException;

public class GlobalStoreManager {
	private static final Object initLock = new Object();
	//private ExecutorService loadWorkers;
	private ExecutorService storeWorkers; // To store blocks in the global store
	private GlobalBackend backend; // To interact with the global store
	
	private static GlobalStoreManager instance;
	
	private GlobalStoreManager() {
		//loadWorkers = Executors.newFixedThreadPool(Constants.numWorkersLoadFromGlobal);
		storeWorkers = Executors.newFixedThreadPool(Constants.numWorkersStoreToGlobal);
		backend = new S3Backend();
	}
	
	public static GlobalStoreManager instance() {
		if (instance == null) {
			synchronized(initLock) {
				if (instance == null)
					instance = new GlobalStoreManager();
			}
		}
		
		return instance;
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
	
	public void store(Block srcBlock, SyncCompleteListener listener) {
		if (srcBlock.markInGlobalQueue()) { // The block is already in the queue or being processed
			System.out.println("[GSM] Already in queue: " + srcBlock.id());
			return;
		}
		
		storeWorkers.submit(() -> storeToGlobal(srcBlock, listener));
		
		//Future<?> future = storeWorkers.submit(() -> storeToGlobal(srcBlock));
		/*try {
			if (future.get() != null) { //Returns null on success
				throw new KawkabException("Unbale to store block in the global store. Block: " + srcBlock.id().name());
			}
		} catch (ExecutionException | InterruptedException e) {
			throw new KawkabException(e);
		}*/
	}
	
	private void storeToGlobal(Block srcBlock, SyncCompleteListener listener) {
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
			System.out.println("[GSM] Global bit dirty, resubmitting block: " + srcBlock.id());
			store(srcBlock, listener);
			return;
		}
		
		try {
			listener.notifyStoreComplete(srcBlock, successful);
		} catch (KawkabException e) {
			e.printStackTrace();
		}
	}
	
	public void shutdown() {
		if (storeWorkers != null) {
			storeWorkers.shutdown();
			try {
				while (!storeWorkers.awaitTermination(3, TimeUnit.SECONDS)) { //Wait until all workers finish their work.
					System.err.println("S3 backend: Unable to close all worker threads.");
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			} finally {
				storeWorkers.shutdownNow();
			}
		}
		
		backend.shutdown();
	}
}
