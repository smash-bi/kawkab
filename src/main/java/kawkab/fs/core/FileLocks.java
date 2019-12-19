package kawkab.fs.core;

import com.google.common.util.concurrent.Striped;

import java.util.concurrent.locks.Lock;

public class FileLocks {
	private static FileLocks instance;
	private Striped<Lock> locks;
	
	private FileLocks() {
		locks = Striped.lazyWeakLock(65535);
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
	public Lock grabLock(BlockID blockID) { //FIXME: This is not a good approach. We should not give the lock to the caller
		assert blockID != null : "BlockID should not be null";

		return locks.get(blockID.perBlockTypeKey());
	}
}
