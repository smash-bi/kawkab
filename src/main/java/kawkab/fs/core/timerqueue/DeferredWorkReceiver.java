package kawkab.fs.core.timerqueue;

public interface DeferredWorkReceiver<T> {
	void deferredWork(T item) ;
}
