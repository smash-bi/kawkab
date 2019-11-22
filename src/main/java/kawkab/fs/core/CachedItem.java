package kawkab.fs.core;

import java.util.concurrent.atomic.AtomicInteger;

public class CachedItem {
	private AtomicInteger refCount; 	// It is not an atomic or volatile variable because this variable is only accessed from the
							// critical section in the CustomCache.acquireBlock() and releaseBlock() functions.
	private final Block block;

	CachedItem(Block block){
		refCount = new AtomicInteger(0);
		this.block = block;
	}
	
	void incrementRefCnt() {
		int val = refCount.incrementAndGet();
		assert val > 0;
	}
	
	void decrementRefCnt() {
		int val = refCount.getAndDecrement();
		assert val > 0;
	}
	
	int refCount() {
		return refCount.get();
	}
	
	public Block block(){
		return block;
	}
}
