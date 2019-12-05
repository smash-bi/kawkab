package kawkab.fs.core;

import java.util.concurrent.atomic.AtomicInteger;

public class CachedItem {
	private static final ApproximateClock clock = ApproximateClock.instance();
	private AtomicInteger refCount; 	// It is not an atomic or volatile variable because this variable is only accessed from the
							// critical section in the CustomCache.acquireBlock() and releaseBlock() functions.
	private final Block block;
	private long accessTime;

	CachedItem(Block block){
		refCount = new AtomicInteger(0);
		this.block = block;
	}
	
	void incrementRefCnt() {
		int val = refCount.incrementAndGet();
		accessTime = clock.currentTime();
		assert val > 0 : "val !> 0 : " + val;
	}
	
	void decrementRefCnt() {
		int val = refCount.getAndDecrement();
		accessTime = clock.currentTime();
		assert val > 0 : "val !> 0 : " + val;
	}
	
	int refCount() {
		return refCount.get();
	}
	
	Block block(){
		return block;
	}

	long accessTime() {
		return accessTime;
	}
}
