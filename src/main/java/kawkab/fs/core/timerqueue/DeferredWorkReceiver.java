package kawkab.fs.core.timerqueue;

import kawkab.fs.core.exceptions.KawkabException;

public interface DeferredWorkReceiver<T> {
	void deferredWork(T item) throws KawkabException;
}
