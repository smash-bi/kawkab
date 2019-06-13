package kawkab.fs.core.timerqueue;

import kawkab.fs.core.TransferQueue;
import kawkab.fs.core.exceptions.KawkabException;

/**
 * A FIFO queue to expire the segments and return them to the cache. This implementation potentially returns items
 * to the cache much slower than required in some cases. As a result, the cache have some items that are eligible
 * for eviction but the cache cannot evict them as their reference count is greater than 0. .
 */
public class TimerQueue {
	private final Thread processorThr;
	private TransferQueue<TimerQueueItem> buffer;
	private volatile boolean working = true;
	
	public TimerQueue() {
		buffer = new TransferQueue<>();
		processorThr = new Thread("SegmentTimerQueueThread") {
			public void run() {
				processSegments();
			}
		};
		
		processorThr.start();
	}
	
	/**
	 * Enable the timer associated with the TimeQueueItem. Moreover, add the item in the TimeQueue so that the TimerQUeue
	 * can call item.deferredWork() some time after the timer expires.
	 *
	 * @param item
	 * @param timeoutMs
	 */
	public void enableAndAdd (TimerQueueItem item, long timeoutMs) {
		assert working;
		
		item.enable(timeoutMs);
		buffer.add(item);
	}
	
	/**
	 * Try to disable the timer of the item if the timer has not expired. Note that the user should not update the item
	 * if disable fails. Instead, the user should assume that item.deferredWork() is eventually called.
	 *
	 * @param item
	 * @return true if the timer was not expired and now the timer is disabled successfully. Otherwise, returns false.
	 */
	public boolean tryDisable(TimerQueueItem item) {
		return item.disableIfNotExpired();
	}
	
	/**
	 * Takes items from the queue,
	 */
	private void processSegments() {
		TimerQueueItem next = null;
		
		while(working) {
			next = buffer.poll();
			if (next == null) {
				try { //Sleeping because the buffer is non-blocking
					Thread.sleep(1); //1ms is chosen randomly. It doesn't impact the performance, but a large value may result in slower expiry of the segments
				} catch (InterruptedException e) {}
				continue;
			}
		
			try {
				process(next);
			} catch (KawkabException e) {
				e.printStackTrace();
				continue;
			}
		}
		
		// Some items may have been left in the queue during closing. We have to retrieve those items and process
		// them before exit.
		while((next = buffer.poll()) != null) {
			try {
				process(next);
			} catch (KawkabException e) {
				e.printStackTrace();
			}
		}
	}
	
	private void process(TimerQueueItem item) throws KawkabException {
		long ret = item.tryExpire();
		
		while (ret > 0) { //While (!EXPIRED && !DISABLED), which implies VALID, which implies a positive value greater than zero
			try {
				Thread.sleep(ret);
			} catch (InterruptedException e) {}
			
			ret = item.tryExpire();
		}
		
		if (ret == ItemTimer.EXPIRED)
			item.deferredWork();
	}
	
	public void waitUntilEmpty() {
		int size;
		while((size = buffer.size()) > 0) {
			System.out.println("Waiting until STQ becomes empty, current size = " + size);
			try {
				Thread.sleep(1);
			} catch (InterruptedException e) {
			}
		}
	}
	
	public void shutdown() {
		System.out.println("Closing TimerQueue");
		working = false;
		
		processorThr.interrupt();
		try {
			processorThr.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		buffer.shutdown();
	}
}
