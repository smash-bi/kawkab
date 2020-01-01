package kawkab.fs.core.tq;

public class TransferableWrapper<T> {
	enum Status {
		INQ, OUTQ, DISABLED, EXPIRED
	}
	
	protected Status status;
	protected T item;
	
	public TransferableWrapper(T newItem) {
		status = Status.OUTQ;
		item = newItem;
	}
	
	public synchronized T getItem() {
		return item;
	}
	
	/**
	 * Wrappers should not be shared. It can be reused once it has been removed from the queue (EXPIRED)
	 */
	public synchronized void reset(T newItem) throws InterruptedException {
		while (status != Status.EXPIRED) {
			wait(); // Can't reuse until it has been popped from the queue
		}
		// Assign a new item and change the state back to OUTQ
		item = newItem;
		status = Status.OUTQ;
	}
		
	protected synchronized boolean disable() {
		if (expiredOrOut()) return false;
		status = Status.DISABLED;
		return true;
	}
	
	protected synchronized boolean enable() {
		if (expiredOrOut()) return false;
		status = Status.INQ;
		return true;
	}
		
	protected synchronized T expire() {
		if (status == Status.OUTQ) {
			return null; // Can't expire an item that is not in the queue
		}
		status = Status.EXPIRED;
		notifyAll();
		T ret = getItem();
		item = null;
		return ret;
	}
	
	protected boolean expiredOrOut() {
		return (status == Status.EXPIRED || status == Status.OUTQ);	
	}
	
	protected boolean expiredOrDisabled() {
		return (status == Status.EXPIRED || status == Status.DISABLED);
	}
}
