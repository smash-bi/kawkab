package kawkab.fs.core;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public abstract class Block /*implements AutoCloseable*/ {
	public enum BlockType {
		DataBlock,  // A segment in a data block 
		InodeBlock, 
		IbmapBlock;
		public static final BlockType values[] = values();
	}
	
	private final Lock lock;
	private final Lock localStoreSyncLock;
	private final Condition syncWait; // To wait until the block is synced to the local store and canbe evicted from the cache.
	private int dirtyCount; // Keeps track of the number of times the block is udpated. It helps in keeping track of the 
	                        // updated bytes and flush only the updated bytes to the local/global store. 
	
	//private AtomicBoolean storedLocally;
	//private AtomicBoolean storedGlobally;
	
	protected boolean loadedInMem = false; // Indicates whether this block's data has been loaded successfully from the 
	                                       // local or the global storage. If data is loaded or the block is empty, data 
	                                       // in this block can be updated/appended and read. Otherwise, 
	                                       // the block cannot be updated/appended or read. 
	
	protected int initialFilledBytes;
	protected int currentFilledBytes;
	
	private final BlockID id;
	//protected Cache cache = Cache.instance(); //FIXME: This creates circular dependencies, which can lead to deadlocks
	//protected BlockType type; //Used for debugging only.
	
	public Block(BlockID id) {
		this.id = id;
		//this.type = type;
		lock = new ReentrantLock();
		localStoreSyncLock = new ReentrantLock();
		syncWait = localStoreSyncLock.newCondition();
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
	//abstract String name();
	
	/**
	 * @return The path of the block local to the current machine, which may be different from the
	 * path global to the distributed filesystem. The returned path is an absolute file path.
	 */
	//abstract String localPath();
	
	protected abstract boolean shouldStoreGlobally();
	
	/**
	 * Load the content of the block from the given ByteBuffer
	 * @param buffer
	 * @throws IOException
	 */
	public abstract void loadFrom(ByteBuffer buffer) throws IOException;
	
	/**
	 * Load the contents of the block from channel.
	 * @param channel
	 * @throws IOException
	 */
	public abstract void loadFrom(ReadableByteChannel channel)  throws IOException;
	
	/**
	 * Store contents of the block to channel. This function stores only the
	 * updated bytes in the channel.
	 * @param channel
	 * @return Number of bytes written to the channel.
	 * @throws IOException
	 */
	public abstract int storeTo(WritableByteChannel channel)  throws IOException;
	
	/**
	 * Stores the complete block in the channel.
	 * @param channel
	 * @return Number of bytes written to the channel.
	 * @throws IOException
	 */
	public abstract int storeFullTo(WritableByteChannel channel) throws IOException;
	
	/**
	 * Loads contents of this block from InputStream in.
	 * @param in
	 * @return Number of bytes read from the input stream.
	 * @throws IOException
	 */
	//abstract int fromInputStream(InputStream in) throws IOException;
	
	/*
	 * Returns a ByteArrayInputStream that wraps around the byte[] containing the block in bytes. GlobalProcessor
	 * calls this function to store the block in the global store. Note that no guarantees are made about the concurrent
	 * modification of the block while the block is read from the stream.
	 */
	public abstract ByteArrayInputStream getInputStream();

	/**
	 * LocalProcessor calls this function to set the position of the FileChannel before calling block.storeTo(channel)
	 * function. This is not a good approach because it will cause problem if multiple workers from the LocalProcessor
	 * concurrently try to flush the segment on disk.
	 * 
	 * @return Returns the byte offset in the segment at which the data will be appended.
	 */
	public abstract int appendOffsetInBlock();
	
	/**
	 * Number of bytes this block is taking in memory. 
	 * @return
	 */
	public abstract int memorySizeBytes();
	
	/**
	 * @return Size of the block in bytes when the complete block is serialized.
	 */
	public abstract int sizeWhenSerialized();
	
	@Override
	public String toString(){
		//return name();
		return id.name();
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
	public boolean dirty(){
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
	
	public int dirtyCount() {
		return dirtyCount;
	}
	
	/**
	 * Clears the dirty bit of this block.
	 */
	public void clearDirty(int count) {
		localStoreSyncLock.lock();
		try {
			dirtyCount -= count;
			if (dirtyCount == 0) {
				syncWait.signal();
			}
			
			assert dirtyCount >= 0;
			
		} finally {
			localStoreSyncLock.unlock();
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
	void close(){
		cache.releaseBlock(this.id());
	}*/
	
	/**
	 * The caller thread blocks until this block is synced to the storage medium.
	 * @throws InterruptedException 
	 */
	public void waitUntilSynced() throws InterruptedException {
		localStoreSyncLock.lock();
		try {
			while (dirtyCount > 0) {
				syncWait.await(); //FIXME: This should be the wait condition from the Cache.cacheLock.
			}
		} finally {
			localStoreSyncLock.unlock();
		}
	}
	
	/**
	 * The caller thread blocks until the block is stored in the Global store.
	 * @throws InterruptedException
	 *//*
	public void waitUntilStoredGlobally() throws InterruptedException {
		lock();
		try {
			while (dirtyCount > 0) {
				syncWait.await(); //FIXME: This should be the wait condition from the Cache.cacheLock. 
			}
		} finally {
			unlock();
		}
	}*/
}
