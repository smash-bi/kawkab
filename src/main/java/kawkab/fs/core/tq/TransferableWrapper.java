package kawkab.fs.core.tq;

public class TransferableWrapper<T> {
	enum Status {
		INQ, OUTQ, DISABLE
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
	 * Wrappers should not be shared. It can be reused once it has been removed from the queue
	 */
	public synchronized void reset(T newItem) throws InterruptedException {
		while (status != Status.OUTQ) {
			wait(); // Can't reuse until it has been popped from the queue
		}
		item = newItem;
	}
		
	protected synchronized boolean disable() {
		if (status == Status.OUTQ) {
			return false; // Has already been removed
		}
		status = Status.DISABLE;
		return true;
	}	
}
