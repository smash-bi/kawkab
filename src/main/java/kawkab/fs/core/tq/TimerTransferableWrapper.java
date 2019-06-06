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
	
	private void resetTimeouts() {
		timeoutTS     = -1;
		nextTimeoutTS = -1;
	}
	
	public synchronized void reset(T newItem) throws InterruptedException {
		super.reset(newItem);
		resetTimeouts();
	}

	protected synchronized void addToTransferQueue(TimerTransferQueue<T> q, long timestamp) {
		if (status == Status.DISABLE || status == Status.INQ) {
			status = Status.INQ;
			nextTimeoutTS = timestamp;
		} else {
			// Outside of the queue
			status = Status.INQ;
			timeoutTS = timestamp;
			nextTimeoutTS = -1;
			q.addFinish(this); // Callback to finish the add while holding this lock
		}
	}
	
	protected synchronized long getTimeout() {
		return timeoutTS;
	}
	
	private T resetAndRemoveFromQ() {
		status = Status.OUTQ;
		resetTimeouts();
		T ret = item;
		item = null;
		notifyAll();
		return ret;
	}

	// Need to check if there is a pending timeout
	protected synchronized T removeFromTransferQueue(TimerTransferQueue<T> q) {
		if (status == Status.INQ) {
			if (nextTimeoutTS == -1) {
				return resetAndRemoveFromQ();
			} else {
				timeoutTS = nextTimeoutTS;
				nextTimeoutTS = -1;
				q.addFinish(this);
			}
		} else if (status == Status.DISABLE) {
			resetAndRemoveFromQ();
		} else if (status == Status.OUTQ) {
			assert(false);
		}		
		return null;
	}
	
	protected synchronized boolean enable(long timestamp) {
		// TODO: Implement this
		if (status == Status.OUTQ) {
			return false; // Has already been removed
		}
		status = Status.INQ;
		nextTimeoutTS = timestamp;
		return true;
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
}

/*
public class TimerTransferableWrapper implements TransferableItem, Comparable<TimerTransferableWrapper> {
	private long timeoutTimestamp;
	private boolean updatePending;
	private long pendingTimeoutTimestamp;
	
	// This field is managed by the transfer queue. Should only be accessed while
	// holding the transfer queue's lock.
	private boolean inTransferQueue;
	
	public TimerTransferableWrapper(long timestamp) {
		this.timeoutTimestamp = timestamp;
		// Use the default values for the other fields
	}
	
	public synchronized boolean applyUpdate() {
		if (updatePending) {
			timeoutTimestamp = pendingTimeoutTimestamp;
			updatePending = false;
			return true;
		}
		return false;
	}
	
	public synchronized void addPendingTimestamp(long timestamp) {
		updatePending = true;
		pendingTimeoutTimestamp = timestamp;
	}
			
	public void setTransferStatus(boolean status) {
		inTransferQueue = status;
	}

	public boolean getTransferStatus() {
		return inTransferQueue;
	}

	public int compareTo(TimerTransferableItem other) {
		if (timeoutTimestamp < other.timeoutTimestamp) {
			return -1;
		} 
		if (timeoutTimestamp > other.timeoutTimestamp) {
			return 1;
		}
		return 0;
	}
}
*/