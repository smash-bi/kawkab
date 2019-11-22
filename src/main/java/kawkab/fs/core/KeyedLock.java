package kawkab.fs.core;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

public final class KeyedLock<T> {
	private Map<T, KeyLock> locks = new HashMap<>();

	public void lock(T key) {
		KeyLock keyLock = null;
		synchronized (locks) {
			keyLock = locks.get(key);
			if (keyLock == null) {
				keyLock = new KeyLock();
				locks.put(key, keyLock);
			}
			
			keyLock.count++;
		}
		
		keyLock.lock.lock();
	}
	
	public void unlock(T key) {
		KeyLock keyLock = null;
		synchronized (locks) {
			keyLock = locks.get(key);
			
			if (keyLock == null)
				return;
			
			if (keyLock.count == 1) //We need to have a lock on "locks" map for this counter.
				locks.remove(key);
			
			keyLock.lock.unlock(); //We can move this unlock() outside of the synchronized block. 
			                      //However, we will have to synchronize again to check and remove the key 
			                      //from the "locks" map, which may have higher performance overhead.
		}
	}
	
	private class KeyLock{
		private ReentrantLock lock;
		private int count;
		private KeyLock() {
			lock = new ReentrantLock();
		}
	}
}
