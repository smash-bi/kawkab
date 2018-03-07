package kawkab.fs.core;

public class CachedItem {
	private int refCount;
	//private Lock lock; // Block level lock
	private Block block;
	
	public CachedItem(Block block){
		refCount = 0;
		this.block = block;
		//lock = new ReentrantLock();
	}
	
	/*public void lock(){
		lock.lock();
	}
	
	public void unlock(){
		lock.unlock();
	}*/
	
	public void incrementRefCnt(){
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
