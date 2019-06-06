package kawkab.fs.core.tq;

/**
 * Unbounded queue that checks if item already exists
 */
public class TransferQueue<T> {
	private ResizeableCircularQueue<FifoTransferableWrapper<T>> backingQueue;
	
	private final static int defaultCapacity = 100;
	
	public TransferQueue(int initCapacity) {
		backingQueue = new ResizeableCircularQueue<FifoTransferableWrapper<T>>(initCapacity);
	}
	
	public TransferQueue() {
		this(defaultCapacity);
	}
	
	/**
	 * Assume that a wrapper can only be in one queue at a time.
	 * 
	 * We want to perform the add operation while holding both the queue's lock and the wrappers's lock. 
	 * This allows us to safely interleave the add operation with methods that only acquire the wrapper's 
	 * lock. Currently this is not necessary since we assume that there is only one owner for a 
	 * TransferableWrapper, but this can change. It also reduces the number of lock acquires and releases.
	 * 
	 * @param wrap The wrapper object that we want to add to the queue.
	 */
	public synchronized void add(FifoTransferableWrapper<T> wrap) {
		wrap.addToTransferQueue(this);
	}
	
	/**
	 * Callback that is called by the wrapper, which in turn was called by add.
	 * Must be protected by both the queue's monitor and the wrapper's monitor.
	 * TODO: This should be described in a separate interface.
	 * 
	 * @param wrap The wrapper object that we want to add to the queue.
	 */
	protected void addFinish(FifoTransferableWrapper<T> wrap) {
		backingQueue.add(wrap);
		notify();
	}
	
	/**
	 * Convenience function. If enable fails, then add
	 * 
	 * @param wrap The wrapper object that we want to enable/add.
	 */
	public void enableOrAdd(FifoTransferableWrapper<T> wrap) {
		if (!enable(wrap)) {
			add(wrap);
		}
	}
	
	public boolean disable(FifoTransferableWrapper<T> wrap) {
		return wrap.disable();
	}
	
	public boolean enable(FifoTransferableWrapper<T> wrap) {
		return wrap.enable();
	}
	
	public synchronized T take() throws InterruptedException{
		int index;
		while ((index = backingQueue.frontIndex()) == -1) {
			wait();
		}
		return removeFromFront(index);
	}
	
	/*
	public synchronized T poll() {
		int index = backingQueue.frontIndex();
		if (index == -1) {
			return null;
		}
		return removeFromFront(index);
	}
	*/
	
	private T removeFromFront(int frontIndex) {
		FifoTransferableWrapper<T> wrap = backingQueue.getAtIndex(frontIndex);
		backingQueue.pop();
		return wrap.removeFromTransferQueue();
	}
	
	public synchronized int size() {
		return backingQueue.size();
	}
}
