package kawkab.fs.core.tq;


/**
 *
 */
public class TimerTransferableWrapper<T> extends TransferableWrapper<T> implements Comparable<TimerTransferableWrapper<T>> {
	protected long timeoutTS;
	protected long nextTimeoutTS;		
	
	public TimerTransferableWrapper(T newItem) {
		super(newItem);
		resetTimeouts();
	}
		
	public synchronized void reset(T newItem) throws InterruptedException {
		super.reset(newItem);
		resetTimeouts();
	}

	protected synchronized boolean addToTransferQueue(TimerTransferQueue<T> q, long timestamp) {
		if (expiredOrDisabled()) {
			return false; // Called while the wrapper is in an unexpected state
		} else if (status == Status.INQ){
			nextTimeoutTS = timestamp; // Just update the next timestamp
		} else {
			assert (status == Status.OUTQ);
			status = Status.INQ;
			timeoutTS = timestamp;
			nextTimeoutTS = -1;
			q.addFinish(this); // Callback to finish the add while holding this lock
		}
		return true;
	}
	
	protected synchronized long getTimeout() {
		return timeoutTS;
	}
	
	private T expireWrapper() {
		status = Status.EXPIRED;
		resetTimeouts();
		T ret = item;
		item = null;
		notifyAll();
		return ret;
	}

	protected synchronized T removeFromTransferQueue(TimerTransferQueue<T> q) {
		if (status == Status.INQ) {
			if (nextTimeoutTS == -1) {
				return expireWrapper();
			} else {
				timeoutTS = nextTimeoutTS;
				nextTimeoutTS = -1;
				q.addFinish(this);
			}
		} else if (status == Status.DISABLED) {
			status = Status.OUTQ;
		} else if (status == Status.EXPIRED) {
			// This can happen if complete was called, and the item has already
			// been processed. Just clean up the wrapper and do nothing. We don't
			// even need to notify since that occurred at the earlier expire call.
		} else if (status == Status.OUTQ) {
			assert(false);
		}
		return null;
	}
	
	protected synchronized T expire() {
		T item = super.expire();
		if (item != null) {
			resetTimeouts();
		}
		return item;
	}
	
	protected synchronized boolean enable(long timestamp) {
		if (super.enable()) {
			nextTimeoutTS = timestamp;
			return true;
		}
		return false;
	}	

	public int compareTo(TimerTransferableWrapper<T> other) {
		if (timeoutTS < other.timeoutTS) {
			return -1;
		} 
		if (timeoutTS > other.timeoutTS) {
			return 1;
		}
		return 0;
	}
	
	private void resetTimeouts() {
		timeoutTS     = -1;
		nextTimeoutTS = -1;
	}

}
