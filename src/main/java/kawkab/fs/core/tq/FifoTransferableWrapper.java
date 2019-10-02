package kawkab.fs.core.tq;

public class FifoTransferableWrapper<T> extends TransferableWrapper<T> {
	public FifoTransferableWrapper(T newItem) {
		super(newItem);
	}

	protected synchronized boolean addToTransferQueue(TransferQueue<T> q) {
		if (expiredOrDisabled()) {
			return false; // Called while the wrapper is an unexpected state
		} else if (status == Status.OUTQ) {
			status = Status.INQ;
			q.addFinish(this); // Callback to finish the add while holding this lock
		}
		return true;
	}

	protected synchronized T removeFromTransferQueue() {
		if (status == Status.INQ) {
			status = Status.EXPIRED;
			T ret = item;
			item = null;  
			notifyAll();  // Wake up threads waiting for this wrapper to expire (so it can be reused)
			return ret;
		} else if (status == Status.DISABLED) {
			status = Status.OUTQ;
			return null;   // Remove from the queue but don't expire.
		} else if (status == Status.EXPIRED) {
			// This state is possible if we support a mechanism to immediately
			// expire an wrapper while still leaving it inside the queue.
			return null;
		}
		// Should never call remove in any other state
		assert(false);
		return null;
	}
}
