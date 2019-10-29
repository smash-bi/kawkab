package kawkab.fs.core.timerqueue;

public interface TimerQueueIface {
	void enableAndAdd (TimerQueueItem item, long timeoutMs);
	boolean tryDisable(TimerQueueItem item);
	void waitUntilEmpty();
	public void shutdown();
}
