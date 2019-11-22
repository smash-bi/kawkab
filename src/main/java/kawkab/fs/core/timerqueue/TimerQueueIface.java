package kawkab.fs.core.timerqueue;

public interface TimerQueueIface {
	void enableAndAdd (TimerQueueItem item, long timeoutMs);

	/**
	 * Try to disable the timer of the item if the timer has not expired. Note that the user should not update the item
	 * if disable fails. Instead, the user should assume that item.deferredWork() is eventually called.
	 *
	 * @param item
	 * @return true if the timer was not expired and now the timer is disabled successfully. Otherwise, returns false.
	 */
	boolean tryDisable(TimerQueueItem item);
	void waitUntilEmpty();
	public void shutdown();
}
