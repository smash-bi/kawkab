package kawkab.fs.core;

import java.util.concurrent.Semaphore;

import com.google.common.util.concurrent.Striped;

public class FileLocks {
	private static FileLocks instance;
	private Striped<Semaphore> locks;
	
	private FileLocks() {
		locks = Striped.lazyWeakSemaphore(20, 1);
	}
	
	public static synchronized FileLocks instance() {
		if (instance == null) {
			instance = new FileLocks();
		}
		
		return instance;
	}
	
	/**
	 * Locks the file
	 * @param blockID
	 * @return
	 * @throws InterruptedException 
	 */
	public void lockFile(BlockID blockID) throws InterruptedException {
		locks.get(blockID.perBlockTypeKey()).acquire();
	}
	
	public void unlockFile(BlockID blockID) {
		locks.get(blockID.perBlockTypeKey()).release();
	}
}
