package kawkab.fs.core;

import com.google.common.base.Stopwatch;
import kawkab.fs.utils.LatHistogram;

import java.io.File;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class LocalStoreCache {
	private int highMark;
	private int lowMark;
	private final LocalCache cache;
	private LocalStoreDB lsd;
	private Semaphore storePermits;
	private int cacheSize;
	private int toEvict;

	public LocalStoreCache(int cacheSize, LocalStoreDB lsd, Semaphore permits) {
		cache = new LocalCache(cacheSize);
		this.lsd = lsd;
		this.storePermits = permits;
		this.cacheSize = cacheSize;

		lowMark = (int)(0.5*cacheSize);
		highMark = (int)(0.8*cacheSize);
		toEvict = (int)(0.005*cacheSize);

		runEvictor();
	}

	public void evict(BlockID id) {
		synchronized (cache) {
			/*if (cache.size() == cacheSize) {
				deleteBlock(id);
				cache.notifyAll();
				return;
			}*/

			cache.put(id, false);

			int size = occupied();
			if (size > highMark && cache.size() > 1) {
				cache.notify();
			}
		}
	}

	public void touch(BlockID id) {
		cache.get(id);
	}

	private void deleteBlock(BlockID id) {
		if (lsd.removeEntry(id) == null) {
			return;
		}

		File file = new File(id.localPath());
		if (!file.delete()) {
			System.err.println("[LEQ] Unable to delete the file: " + file.getAbsolutePath());
		}

		storePermits.release();
	}

	public void makeSpace() {
		if (occupied() < highMark)
			return;

		Stopwatch sw = Stopwatch.createStarted();
		synchronized (cache) {
			if (cache.size() > 0)
				cache.bulkRemove(30);
			else
				return;
			//cache.notifyAll();

			if (cache.size() > 1) {
				cache.notify();
			}
		}
		long elapsed = sw.stop().elapsed(TimeUnit.MILLISECONDS);
		if (elapsed > 1) {
			System.out.println("Local store make space (ms): " + elapsed);
		}
	}

	private int occupied() {
		return cacheSize - storePermits.availablePermits();
	}

	private void runEvictor() {
		Thread evictor = new Thread(() -> {
			while (true) {
				int size = occupied();

				if (size <= lowMark || cache.size() == 0) {
					synchronized (cache) {
						try {
							cache.wait(1000);
							continue;
						} catch (InterruptedException e) {
							break;
						}
					}
				}

				int n = toEvict > occupied()-lowMark ? occupied() - lowMark : toEvict;

				synchronized (cache) {
					cache.bulkRemove(n);
					//cache.notifyAll();
				}
 
				try {
					Thread.sleep(500);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		});
		evictor.setName("LocalStoreCacheEvictor");
		evictor.setDaemon(true);
		evictor.start();
	}

	public int size() {
		return cache.size();
	}

	private class LocalCache extends LinkedHashMap<BlockID, Boolean> {

  		public LocalCache( int cacheSize){
			super(cacheSize, 0.75f, true);
		}

		protected boolean removeEldestEntry (Map.Entry < BlockID, Boolean > eldest) {
			int size = occupied();
  			if (size < highMark)
  				return false;

  			deleteBlock(eldest.getKey());

			return true;
		}

		int bulkRemove(int toEvict) {
  			//long t = System.currentTimeMillis();
			int evicted = 0;
			Iterator<Map.Entry<BlockID, Boolean>> itr = super.entrySet().iterator();
			for (int i=0; i<toEvict; i++) {
				if (!itr.hasNext())
					break;

				deleteBlock(itr.next().getKey());

				itr.remove();
				evicted++;
			}

			/*long e = System.currentTimeMillis() - t;
			if (e > 10) {
				System.out.printf("Removed=%d, size %d, elapsed (ms) %d\n", evicted, size(), e);
			}*/

			return evicted;
		}
	}
}
