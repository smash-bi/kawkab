package kawkab.fs.core;

import java.nio.ByteBuffer;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import kawkab.fs.core.exceptions.InsufficientResourcesException;

public abstract class Block implements AutoCloseable {
	ReadWriteLock rwLock;
	Lock readLock;
	Lock writeLock;
	boolean dirty;
	
	abstract void fromBuffer(ByteBuffer buffer) throws InsufficientResourcesException;
	abstract void toBuffer(ByteBuffer buffer) throws InsufficientResourcesException;
	abstract String name();
	abstract String localPath();
	abstract int blockSize();
	
	protected Block() {
		rwLock = new ReentrantReadWriteLock();
		readLock = rwLock.readLock();
		writeLock = rwLock.writeLock();
	}
	
	void clearDirty(){
		dirty = false;
	}
	
	boolean dirty(){
		return dirty;
	}
	
	@Override
	public void close(){
		if (dirty()){
			Cache.instance().addDirty(this);
		}
	}
	
	void acquireWriteLock(){
		writeLock.lock();
	}
	
	void releaseWriteLock(){
		writeLock.unlock();
	}
	
	void acquireReadLock(){
		readLock.lock();
	}
	
	void releaseReadLock(){
		readLock.unlock();
	}
	
}