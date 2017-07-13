package kawkab.fs.core;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.locks.ReentrantLock;

public class CloseableLock implements Closeable{
	private ReentrantLock lock;
	private int count;
	private long owner;

	public CloseableLock() {
		lock = new ReentrantLock();
	}
	
	public void lock(){
		lock.lock();
		count++;
		owner = Thread.currentThread().getId();
	}
	
	public boolean isOwner(){
		return owner == Thread.currentThread().getId();
	}
	
	public int count(){
		return count;
	}
	
	public void unlock() throws IOException{
		close();
	}

	@Override
	public void close() throws IOException {
		--count;
		lock.unlock();
	}
}
