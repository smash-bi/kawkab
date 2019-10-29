package kawkab.fs.core.timerqueue;

public class NullTimerQueue implements TimerQueueIface {
	@Override
	public void enableAndAdd(TimerQueueItem item, long timeoutMs) {
	}

	@Override
	public boolean tryDisable(TimerQueueItem item) {
		return true;
	}

	@Override
	public void waitUntilEmpty() {
	}

	@Override
	public void shutdown() {
	}
}
