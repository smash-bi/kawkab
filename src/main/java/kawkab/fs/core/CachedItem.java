package kawkab.fs.core;

public class CachedItem {
	private int refCount; 	// It is not an atomic or volatile variable because this variable is only accessed from the 
							// critical section in the CustomCache.acquireBlock() and releaseBlock() functions.
	private final Block block;
	
	public CachedItem(Block block){
		refCount = 0;
		this.block = block;
	}
	
	public void incrementRefCnt() {
		refCount++;
	}
	
	public void decrementRefCnt() {
		refCount--;
		assert refCount >= 0;
	}
	
	public int refCount(){
		return refCount;
	}
	
	public Block block(){
		return block;
	}
}
