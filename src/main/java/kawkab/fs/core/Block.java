package kawkab.fs.core;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public abstract class Block /*implements AutoCloseable*/ {
	public enum BlockType {
		DataBlock,  // A segment in a data block 
		InodeBlock, 
		IbmapBlock
	}
	
	private final Lock lock;
	private final Condition syncWait;
	private int dirtyCount;
	
	private AtomicBoolean storedLocally;
	private AtomicBoolean storedGlobally;
	
	int initialFilledBytes;
	int currentFilledBytes;
	
	final BlockID id;
	//protected Cache cache = Cache.instance(); //FIXME: This creates circular dependencies, which can lead to deadlocks
	//protected BlockType type; //Used for debugging only.
	
	Block(BlockID id) {
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
	
	abstract boolean shouldStoreGlobally();
	
	/**
	 * Load the contents of the block from channel.
	 * @param channel
	 * @throws IOException
	 */
	abstract void loadFrom(ReadableByteChannel channel)  throws IOException;
	
	/**
	 * Store contents of the block to channel. This function stores only the
	 * updated bytes in the channel.
	 * @param channel
	 * @return Number of bytes written to the channel.
	 * @throws IOException
	 */
	abstract int storeTo(WritableByteChannel channel)  throws IOException;
	
	/**
	 * Stores the complete block in the channel.
	 * @param channel
	 * @return Number of bytes written to the channel.
	 * @throws IOException
	 */
	abstract int storeFullTo(WritableByteChannel channel) throws IOException;
	
	/*
	 * Returns a ByteArrayInputStream that wraps around the byte[] containing the block in bytes. GlobalProcessor
	 * calls this function to store the block in the global store. Note that no guarantees are made about the concurrent
	 * modification of the block while the block is read from the stream.
	 */
	abstract ByteArrayInputStream getInputStream();

	/**
	 * LocalProcessor calls this function to set the position of the FileChannel before calling block.storeTo(channel)
	 * function. This is not a good approach because it will cause problem if multiple workers from the LocalProcessor
	 * concurrently try to flush the segment on disk.
	 * 
	 * @return Returns the byte offset in the segment at which the data will be appended.
	 */
	abstract int appendOffsetInBlock();
	
	/**
	 * Number of bytes this block is taking in memory. 
	 * @return
	 */
	abstract int memorySizeBytes();
	
	/**
	 * @return Size of the block in bytes when the complete block is serialized.
	 */
	abstract int sizeWhenSerialized();
	
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
			if (dirtyCount > 0) //TODO: Make the dirtyCount as atomicInteger
				isDirty = true;
		unlock();
		
		return isDirty;
	}
	
	public void markDirty() {
		lock();
		dirtyCount++;
		unlock();
	}
	
	int dirtyCount() {
		return dirtyCount;
	}
	
	/**
	 * Clears the dirty bit of this block.
	 */
	void clearDirty(int count) {
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
	void cleanup() throws IOException {}
	
	/**
	 * This function causes the cache to flush this block if the block is dirty. This function
	 * does not closes this block from writing. It overrides the close function in the AutoCloseable
	 * interface.
	 */
	/*@Override
	void close(){
		cache.releaseBlock(this.id());
	}*/
	
	/**
	 * The caller thread blocks until this block is synced to the storage medium.
	 * @throws InterruptedException 
	 */
	void waitUntilSynced() throws InterruptedException {
		lock();
		try {
			while (dirtyCount > 0) {
				syncWait.await(); //FIXME: This should be the wait condition from the Cache.cacheLock. 
			}
		} finally {
			unlock();
		}
	}
	
	/**
	 * The caller thread blocks until the block is stored in the Global store.
	 * @throws InterruptedException
	 */
	void waitUntilStoredGlobally() throws InterruptedException {
		lock();
		try {
			while (dirtyCount > 0) {
				syncWait.await(); //FIXME: This should be the wait condition from the Cache.cacheLock. 
			}
		} finally {
			unlock();
		}
	}
}
