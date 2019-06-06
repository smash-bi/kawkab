package kawkab.fs.core.tq;

public class FifoTransferableWrapper<T> extends TransferableWrapper<T> {
	public FifoTransferableWrapper(T newItem) {
		super(newItem);
	}

	protected synchronized void addToTransferQueue(TransferQueue<T> q) {
		if (status == Status.DISABLE) {
			status = Status.INQ;
		} else if (status == Status.OUTQ) {
			status = Status.INQ;
			q.addFinish(this); // Callback to finish the add while holding this lock
		}
	}

	protected synchronized T removeFromTransferQueue() {
		assert(status != Status.OUTQ);
		try {
			return status == Status.INQ ? item : null;
		} finally {
			status = Status.OUTQ;
			item = null;
			notifyAll();
		}
	}
	
	protected synchronized boolean enable() {
		if (status == Status.OUTQ) {
			return false; // Has already been removed
		}
		status = Status.INQ;
		return true;
	}	
}
