package kawkab.fs.core;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public abstract class Block /*implements AutoCloseable*/ {
	public enum BlockType {
		DataBlock, InodeBlock, IbmapBlock
	}
	
	private final Lock lock;
	private final Condition syncWait;
	private int dirtyCount;
	
	protected final BlockID id;
	//protected Cache cache = Cache.instance(); //FIXME: This creates circular dependencies, which can lead to deadlocks
	//protected BlockType type; //Used for debugging only.
	
	protected Block(BlockID id) {
		this.id = id;
		//this.type = type;
		lock = new ReentrantLock();
		syncWait = lock.newCondition();
	}
	
	/**
	 * Acquire the values of this block from the given Buffer. This function loads values of the
	 * class variables from the given ByteBuffer. It consumes the number of bytes equal to the
	 * blockSize() function.
	 * @param buffer
	 * @throws InsufficientResourcesException
	 */
	//abstract void fromBuffer(ByteBuffer buffer) throws InsufficientResourcesException;
	
	/**
	 * Serializes this block in the given buffer.
	 * @param buffer
	 * @throws InsufficientResourcesException
	 */
	//abstract void toBuffer(ByteBuffer buffer) throws InsufficientResourcesException;
	
	/**
	 * @return Returns the name of the current block, which can be used as a unique key.
	 */
	abstract String name();
	
	/**
	 * @return The path of the block local to the current machine, which may be different from the
	 * path global to the distributed filesystem. The returned path is an absolute file path.
	 */
	abstract String localPath();
	
	/**
	 * The serialization/deserialization size of this block.
	 * 
	 * @return Returns the size of the block in bytes. When the block is serializes or deserializes,
	 * it consumes the number of bytes equal to the returned value of this function.  
	 */
	abstract int blockSize();
	
	abstract public void loadFrom(ReadableByteChannel channel)  throws IOException;
	
	abstract public void storeTo(WritableByteChannel channel)  throws IOException;
	
	abstract int channelOffset();
	
	
	@Override
	public String toString(){
		return name();
	}
	
	/*public BlockType type(){
		return type;
	}*/
	
	/*public void loadFrom() throws IOException {
		LocalStore store = LocalStore.instance();
		try {
			store.readBlock(this);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void storeTo() throws IOException {
		LocalStore store = LocalStore.instance();
		try {
			store.writeBlock(this);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}*/
	
	
	
	
	/**
	 * @return Returns whether this block has been modified until now or not.
	 */
	boolean dirty(){
		boolean isDirty = false;
		lock();
			if (dirtyCount > 0)
				isDirty = true;
		unlock();
		
		return isDirty;
	}
	
	public void markDirty() {
		lock();
		dirtyCount++;
		unlock();
	}
	
	public int dirtyCount() {
		return dirtyCount;
	}
	
	/**
	 * Clears the dirty bit of this block.
	 */
	public void clearDirty(int count) {
		lock();
		try {
			dirtyCount -= count;
			if (dirtyCount == 0) {
				syncWait.signal();
			}
			
			assert dirtyCount >= 0;
			
		} finally {
			unlock();
		}
	}
	
	public void lock(){
		lock.lock();
	}
	
	public void unlock(){
		lock.unlock();
	}
	
	public BlockID id(){
		return id;
	}
	
	/**
	 * Releases any acquired resources such as file channels
	 */
	public void cleanup() throws IOException {}
	
	/**
	 * This function causes the cache to flush this block if the block is dirty. This function
	 * does not closes this block from writing. It overrides the close function in the AutoCloseable
	 * interface.
	 */
	/*@Override
	public void close(){
		cache.releaseBlock(this.id());
	}*/
	
	/**
	 * The caller thread blocks until this block is synced to the storage medium.
	 * @throws InterruptedException 
	 */
	public void waitUntilSynced() throws InterruptedException {
		lock();
		try {
			while (dirtyCount > 0) {
				syncWait.await();
			}
		} finally {
			unlock();
		}
	}
}