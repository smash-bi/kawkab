package kawkab.fs.core.timerqueue;

import kawkab.fs.core.TransferQueue;
import kawkab.fs.core.exceptions.KawkabException;

/**
 * A FIFO queue to expire the segments and return them to the cache. This implementation potentially returns items
 * to the cache much slower than required in some cases. As a result, the cache have some items that are eligible
 * for eviction but the cache cannot evict them as their reference count is greater than 0. .
 */
public class TimerQueue implements TimerQueueIface {
	private final Thread processorThr;
	private TransferQueue<TimerQueueItem> trnsfrQ;
	private volatile boolean working = true;
	private final String name;

	/**
	 * @param name Name of the queue for debugging and tracing purposes.
	 */
	public TimerQueue(String name) {
		System.out.println("[TQ] Initializing Timer Queue: " + name);
		this.name = name;
		trnsfrQ = new TransferQueue<>(name+"-TfrQ");
		processorThr = new Thread(name+"Worker") {
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
	@Override
	public void enableAndAdd (TimerQueueItem item, long timeoutMs) {
		assert working;

		item.enable(timeoutMs);
		trnsfrQ.add(item);
	}
	
	/**
	 * Try to disable the timer of the item if the timer has not expired. Note that the user should not update the item
	 * if disable fails. Instead, the user should assume that item.deferredWork() is eventually called.
	 *
	 * @param item
	 * @return true if the timer was not expired and now the timer is disabled successfully. Otherwise, returns false.
	 */
	@Override
	public boolean tryDisable(TimerQueueItem item) {
		return item.disableIfNotExpired();
	}
	
	/**
	 * Takes items from the queue,
	 */
	private void processSegments() {
		TimerQueueItem next;
		System.out.println("[TQ] Starting TQ worker: " + Thread.currentThread().getName());
		
		while(working) {
			next = trnsfrQ.poll();
			if (next == null) {
				try { //Sleeping because the trnsfrQ is non-blocking
					Thread.sleep(3); //1ms is chosen randomly. It doesn't impact the performance, but a large value may result in slower expiry of the segments
				} catch (InterruptedException e) {}
				continue;
			}

			//debugPrint(next, "        <-- ");

			try {
				process(next);
			} catch (KawkabException e) {
				e.printStackTrace();
				continue;
			}
		}
		
		// Some items may have been left in the queue during closing. We have to retrieve those items and process
		// them before exit.
		while((next = trnsfrQ.poll()) != null) {
			try {
				process(next);
			} catch (KawkabException e) {
				e.printStackTrace();
			}
		}

		System.out.println("[TQ] TImer Queue worker exit: " + name);
	}
	
	private void process(TimerQueueItem item) throws KawkabException {
		long ret = item.tryExpire();

		if (ret > 0) { //While (!EXPIRED && !DISABLED), which implies VALID, which implies a positive value greater than zero
			try {
				//debugPrint(item, "Sleep:" + ret);
				Thread.sleep(ret);
			} catch (InterruptedException e) {}
			
			ret = item.tryExpire();
		}

		if (ret > 0)
			trnsfrQ.add(item);

		if (ret == ItemTimer.EXPIRED) {
			item.deferredWork();
		}
	}

	@Override
	public void waitUntilEmpty() {
		while(trnsfrQ.size() > 0) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
			}
		}
	}

	@Override
	public void shutdown() {
		System.out.println("Closing TimerQueue");
		working = false;

		waitUntilEmpty();
		
		processorThr.interrupt();
		try {
			processorThr.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		trnsfrQ.shutdown();
	}

	/*private void debugPrint(TimerQueueItem item, String prefix) {
		if (!name.equals("FSTQ"))
			return;

		try {
			Block b = null;
			if (item.getItem() instanceof Block) {
				b = (Block) item.getItem();
			} else if (item.getItem() instanceof FileChannels.FileChannelWrap) {
				FileChannels.FileChannelWrap fcw = (FileChannels.FileChannelWrap)item.getItem();
				System.out.printf("[%s] %s %s\n", name, prefix, fcw);
				return;
			} else {
				System.out.println(item.getItem().getClass());
			}

			if (b != null && b.id().type() != BlockID.BlockType.DATA_SEGMENT) {
				System.out.printf("[%s] %s %s\n", name, prefix, b.id());
			}
		} catch(Exception e){e.getMessage();}
	}*/
}
