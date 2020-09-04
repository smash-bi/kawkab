package kawkab.fs.core.tq;

public class NullTimerTransferQueue<T> extends TimerTransferQueue<T> {
	public NullTimerTransferQueue(String name) {
		super(name);
	}

	public NullTimerTransferQueue() {
		this("Default Null Transfer Queueu");
	}

	public synchronized boolean add(TimerTransferableWrapper<T> wrap, long timeout) { return false; }

	public void enableOrAdd(TimerTransferableWrapper<T> wrap, long timeout) { }

	public boolean disable(TimerTransferableWrapper<T> wrap) { return true; }

	public boolean enable(TimerTransferableWrapper<T> wrap, long timeout) { return false; }

	public synchronized int size() { return 0; }

	public void waitUntilEmpty() { }

	public void shutdown() { }
}
