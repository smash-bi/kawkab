package kawkab.fs.core;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class CachedItem {
	private int refCount;
	private Lock lock;
	private Block block;
	
	public CachedItem(Block block){
		refCount = 0;
		this.block = block;
		lock = new ReentrantLock();
	}
	
	public void lock(){
		lock.lock();
	}
	
	public void unlock(){
		lock.unlock();
	}
	
	public void incrementRefCnt(){
		refCount++;
	}
	
	public void decrementRefCnt() {
		refCount--;
	}
	
	public int refCount(){
		return refCount;
	}
	
	public Block block(){
		return block;
	}
}
