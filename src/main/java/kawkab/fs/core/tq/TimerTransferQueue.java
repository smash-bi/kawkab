package kawkab.fs.core.tq;

import java.util.PriorityQueue;

/**
 * Unbounded queue that checks if an item already exists. Each item is given an
 * expiry time. An item can only be removed from the queue once it has expired.
 * An item's timer can be disabled before its expiry time. An invariant in our
 * system is that the timeout for a wrapper cannot change while the wrapper is
 * in a queue. Instead, the timeout is updated once it is popped out, which
 * means that calling enable with a timeout timestamp that is smaller than the
 * original timeout timestamp will not work properly.
 * 
 * A caller can immediately expire an item by calling "complete".
 */
public class TimerTransferQueue<T> {
	private PriorityQueue<TimerTransferableWrapper<T>> backingQueue;
	//private ResizeableCircularQueue<TimerTransferableWrapper<T>> completeQueue;
	private Thread worker;
	private String name;
	
	private final static int defaultCapacity = 100;
	
	public TimerTransferQueue(String name, int initCapacity) {
		this.name = name;
		backingQueue = new PriorityQueue<TimerTransferableWrapper<T>>(initCapacity);
		//completeQueue = new ResizeableCircularQueue<TimerTransferableWrapper<T>>(initCapacity);
		runWorker();
	}
	
	public TimerTransferQueue(String name) {
		this(name, defaultCapacity);
	}
	
	/**
	 * Assume that a wrapper can only be in one queue at a time.
	 * 
	 * We want to perform the add operation while holding both the queue's lock
	 * and the wrappers's lock. This allows us to safely interleave the add
	 * operation with methods that only acquire the wrapper's lock. Currently
	 * this is not necessary since we assume that there is only one owner for a
	 * TransferableWrapper, but this can change. It also reduces the number of
	 * lock acquires and releases.
	 * 
	 * @param wrap	The wrapper object that we want to add to the queue.
	 */
	public synchronized boolean add(TimerTransferableWrapper<T> wrap, long timeout) {
		return wrap.addToTransferQueue(this, timeout);
	}
	
	/**
	 * Callback that is called by the wrapper, which in turn was called by add.
	 * Must be protected by both the queue's monitor and the wrapper's monitor.
	 * TODO: This should be described in a separate interface.
	 * 
	 * @param wrap 	The wrapper object that we want to add to the queue.
	 */
	protected void addFinish(TimerTransferableWrapper<T> wrap) {
		backingQueue.add(wrap);
		// Wake up threads if this item has the earliest timeout timestamp. 
		// This is because we need to update the sleep period.
		if (backingQueue.peek() == wrap) {
			notifyAll();
		}
	}
	
	/**
	 * Convenience function. If enable fails, then add
	 * 
	 * @param wrap	The wrapper object that we want to enable/add.
	 */
	public void enableOrAdd(TimerTransferableWrapper<T> wrap, long timeout) {
		if (!enable(wrap, timeout)) {
			//System.out.println("Shouldn't get here");
			add(wrap, timeout);
		}
	}
	
	/** 
	 * Completes the item immediately. Wrapper is disabled and should not be reused.
	 * TODO: Add a complete state to the wrapper.
	 * 
	 * @param wrap	The wrapper object that we want t complete.
	 * @return 		Success of operation
	 */
	/*public synchronized boolean complete(TimerTransferableWrapper<T> wrap) {
		T item = wrap.expire();	// Change the state of the wrapper to expire
		if (item == null) return false;
		//completeQueue.add(item);
		completeQueue.add(wrap);
		notifyAll();
		return true;
	}*/
	
	public boolean disable(TimerTransferableWrapper<T> wrap) {
		return wrap.disable();
	}
	
	public boolean enable(TimerTransferableWrapper<T> wrap, long timeout) {
		return wrap.enable(timeout);
	}
	
	private synchronized TimerTransferableWrapper<T> take() throws InterruptedException {
		while (true) {
			// Check to see if there are items in the complete queue.
			/*int completeIndex = completeQueue.frontIndex();
			if (completeIndex != -1) {
				//T item = completeQueue.getAtIndex(completeIndex);
				TimerTransferableWrapper<T> w = completeQueue.getAtIndex(completeIndex);
				completeQueue.pop();
				return w;
			}*/
			// With no items in the complete queue, retrieve an item from
			// the backingQueue. Block if the blockingQueue is empty.
			TimerTransferableWrapper<T> w = backingQueue.peek();
			if (w == null) {
				wait();
			} else {
				long currentTS = System.currentTimeMillis();
				long timeoutTS = w.getTimeout();
				if (timeoutTS <= currentTS) {
					//T ret = backingQueue.poll().removeFromTransferQueue(this);
					TimerTransferableWrapper<T> ret = backingQueue.poll();
					if (ret != null && ret.getItem() != null) {
						boolean expired = ret.removeFromTransferQueue(this);
						if (!expired)
							continue;

						return ret; // Wrapper removed. Return to caller
					} else {
						continue;   // Try again. This item had an updated timeout.
					}
				} else {
					// Timeout not satisfied yet. Wait until it is.
					wait(timeoutTS - currentTS);
				}
			}
		}
	}

	public synchronized int size() {
		return backingQueue.size();
	}

	private void runWorker() {
		worker = new Thread(() -> {
			try {
				while (true) {
					TimerTransferableWrapper<T> w = take();
					assert w != null : "Taken wrapper is null";
					w.performDeferredWork();
				}
				//T next = take();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		});
		worker.start();
	}

	public void shutdown() {
		worker.interrupt();
	}

	public void waitUntilEmpty() {
		while(size() > 0) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
			}
		}
	}
}
