package kawkab.fs.core;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import kawkab.fs.commons.Constants;
import kawkab.fs.core.exceptions.FileNotExistException;
import kawkab.fs.core.exceptions.KawkabException;
import kawkab.fs.core.services.PrimaryNodeServiceClient;

public abstract class Block /*implements AutoCloseable*/ {
	private static GlobalProcessor globalStore = GlobalProcessor.instance();
	private static LocalProcessor localStore = LocalProcessor.instance();
	protected static PrimaryNodeServiceClient primaryNodeService = PrimaryNodeServiceClient.instance();
	
	private final Lock lock;
	
	private final Lock localStoreSyncLock;
	private final Condition syncWait; // To wait until the block is synced to the local store and canbe evicted from the cache.
	
	private long lastGlobalFetchTimeMs = 0; //Clock time in ms when the block was last loaded from the local or the global store
	private long lastPrimaryFetchTimeMs = 0; //Clock time in ms when the block was last loaded from the local or the global store
	private final Lock dataLoadLock; //Lock for loading data in memory, disabling other threads from reading the block until data is loaded
	
	private int dirtyCount; // Keeps track of the number of times the block is udpated. It helps in keeping track of the 
	                        // updated bytes and flush only the updated bytes to the local/global store. 
	
	//private AtomicBoolean storedLocally;
	//private AtomicBoolean storedGlobally;
	
	//protected boolean loadedInMem = false; // Indicates whether this block's data has been loaded successfully from the 
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
		
		lastGlobalFetchTimeMs = 0;
		lastPrimaryFetchTimeMs = 0;
		dataLoadLock = new ReentrantLock();
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
	
	protected abstract void getFromPrimary()  throws FileNotExistException, KawkabException, IOException;
	
	protected void loadBlock() throws KawkabException {
		long now = System.currentTimeMillis();
		if (lastGlobalFetchTimeMs < now - Constants.globalFetchExpiryTimeoutMs
				|| lastPrimaryFetchTimeMs < now - Constants.primaryFetchExpiryTimeoutMs) { // If loaded data has expired
			try {
				dataLoadLock.lock(); // Disable loading from concurrent threads
				
				now = System.currentTimeMillis();
				
				if (lastGlobalFetchTimeMs < now - Constants.globalFetchExpiryTimeoutMs) {
					System.out.println("[Block] Global fetch expired: " + id);
					
					if (id.onPrimaryNode()) { // If this node is the primary writer of the file
						System.out.println("[Block] Load from the local on primary: " + id);
						
						localStore.load(this); // Load data from the local store
						lastGlobalFetchTimeMs = Long.MAX_VALUE; //Never expire the loaded data
						lastPrimaryFetchTimeMs = Long.MAX_VALUE;
						
						return;
					}
					
					/* If never fetched or the last global fetch has timed out, fetch from the global store.
					 * Otherwise, if the last remote fetch has timed out, fetch from the remote node.
					 * Otherwise, don't fetch, data is still fresh. 
					 */
					
					try {
						System.out.println("[Block] Load from the global: " + id);
						
						globalStore.load(this); // First try loading data from the global store
						lastGlobalFetchTimeMs = now;
						lastPrimaryFetchTimeMs = 0; //Indicates that this node has not fetched this segment from the primary node
						
						return;
					} catch (FileNotExistException e) { //If the block is not in the global store yet
						System.out.println("[Block] Not found in the global: " + id);
						lastGlobalFetchTimeMs = 0;
					}
				}
				
				// Either primary data fetch has timed out or the global fetch was timed out but the global fetch has failed
				// In both cases, we need to fetch from the primary node
				
				System.out.println("[Block] Primary fetch expired or not found from the global: " + id);
				
				try {
					System.out.println("[Block] Load from the primary: " + id);
					getFromPrimary();
					lastGlobalFetchTimeMs = (lastGlobalFetchTimeMs==0 ? now : lastGlobalFetchTimeMs);
					lastPrimaryFetchTimeMs = now;
				} catch (FileNotExistException ke) {
					// Check again from the global store because the primary may have deleted the 
					// block after copying to the global store
					System.out.println("[Block] Not found on the primary, trying again from the global: " + id);
					globalStore.load(this); 
					lastGlobalFetchTimeMs = now;
					lastPrimaryFetchTimeMs = 0;
				} catch (IOException ioe) {
					System.out.println("[Block] Not found in the global and the primary: " + id);
					throw new KawkabException(ioe);
				}
			} finally {
				dataLoadLock.unlock();
			}
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
