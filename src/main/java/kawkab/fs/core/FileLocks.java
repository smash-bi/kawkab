package kawkab.fs.core;

import com.google.common.util.concurrent.Striped;

import java.util.concurrent.locks.Lock;

public class FileLocks {
	private static FileLocks instance;
	private Striped<Lock> locks;
	
	private FileLocks() {
		locks = Striped.lazyWeakLock(1000); //1000 is a sufficiently large number because usually the number of cores and concurrent threads is small
	}
	
	public static synchronized FileLocks instance() {
		if (instance == null) {
			instance = new FileLocks();
		}
		
		return instance;
	}
	
	/**
	 * Returns a lock specific to the blockID
	 *
	 * @param blockID
	 * @return
	 * @throws InterruptedException 
	 */
	public Lock grabLock(BlockID blockID) { //FIXME: This is not a good approach. We should not give away the lock
		return locks.get(blockID.perBlockTypeKey());
	}
}
