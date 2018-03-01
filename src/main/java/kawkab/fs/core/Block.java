package kawkab.fs.core;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import kawkab.fs.commons.Constants;
import kawkab.fs.core.exceptions.FileNotExistException;
import kawkab.fs.core.exceptions.KawkabException;
import kawkab.fs.core.services.PrimaryNodeServiceClient;

/**
 * Global dirty count is used to manage the locally and globally persisted copy of the block. If the block is globally
 * persisted, it can be deleted from the local storage.
 * The dirty count starts with zero, which means the block is in cache, not persisted locally, and not persisted
 * globally. The dirty count is atomically incremented after 1) the block is stored locally, 2) the block is stored
 * globally, 3) the block is evicted from the cache.
 * The dirty count's value equals to 1 means that the block is in the cache, is stored locally, and is not stored
 * locally. This is because the block cannot be evicted from the cache until it is persisted locally. Moreover, the
 * block's global persistence is not initiated until the block is persisted locally. Therefore, the first increment
 * is performed by the local  
 */

public abstract class Block /*implements AutoCloseable*/ {
	private static GlobalStoreManager globalStoreManager;
	private static LocalStoreManager localStoreManager;
	protected static PrimaryNodeServiceClient primaryNodeService = PrimaryNodeServiceClient.instance();
	
	private final Lock lock;
	
	private final Lock storeSyncLock; // Lock to prevent cache eviction before syncing to the local store
	private final Condition syncWait; // To wait until the block is synced to the local store and canbe evicted from the cache.
	
	private long lastGlobalFetchTimeMs = 0; //Clock time in ms when the block was last loaded from the local or the global store
	private long lastPrimaryFetchTimeMs = 0; //Clock time in ms when the block was last loaded from the local or the global store
	private final Lock dataLoadLock; //Lock for loading data in memory, disabling other threads from reading the block until data is loaded
	
	private AtomicInteger globalDirtyCnt;
	private AtomicInteger localDirtyCnt; // Keeps track of the number of times the block is udpated. It helps in keeping track of the 
	                        // updated bytes and flush only the updated bytes to the local/global store. 
	                        // TODO: Change this to a dirty bit. This can be achieved by an AtomicBoolean instead of
	                        // an AtomicInteger
	
	private AtomicBoolean inLocalQueue; // The block is in a queue for local persistence
	private AtomicBoolean inGlobalQueue; // The block is in a queue for global persistence
	
	private AtomicBoolean inLocalStore; // The blocks is currently in the local store (can be in more places as well)
	private AtomicBoolean inCache;      // The block is in cache
	
	protected int initialFilledBytes;
	protected int currentFilledBytes;
	
	private final BlockID id;
	//protected Cache cache = Cache.instance(); //FIXME: This creates circular dependencies, which can lead to deadlocks
	//protected BlockType type; //Used for debugging only.
	
	static {
		try {
			globalStoreManager = GlobalStoreManager.instance();
			localStoreManager = LocalStoreManager.instance(); //FIXME: Handle exception properly
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public Block(BlockID id) {
		this.id = id;
		//this.type = type;
		lock = new ReentrantLock();
		
		storeSyncLock = new ReentrantLock();
		syncWait = storeSyncLock.newCondition();
		
		lastGlobalFetchTimeMs = 0;
		lastPrimaryFetchTimeMs = 0;
		dataLoadLock = new ReentrantLock();
		
		localDirtyCnt  = new AtomicInteger(0);
		globalDirtyCnt = new AtomicInteger(0);
		inLocalQueue   = new AtomicBoolean(false);
		inGlobalQueue  = new AtomicBoolean(false);
		inLocalStore   = new AtomicBoolean(false);
		inCache        = new AtomicBoolean(true);
	}
	
	
	/**
	 * Indicates if the block should be stored in the global store or not, regardless of its dirty bits status.
	 * @return
	 */
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
	
	public void markDirty() {
		localDirtyCnt.incrementAndGet();
		globalDirtyCnt.incrementAndGet();
	}
	
	/**
	 * @return Returns whether this block has been modified until now or not.
	 */
	public boolean isLocalDirty(){
		return localDirtyCnt.get() > 0;
	}
	
	public int localDirtyCount() {
		return localDirtyCnt.get();
		//return dirtyCount;
	}
	
	/**
	 * Clears the dirty bit of this block.
	 */
	public int clearAndGetLocalDirty(int count) {
		int cnt = localDirtyCnt.addAndGet(-count);
		
		assert cnt >= 0;
		
		if (cnt == 0) {
			try {
				storeSyncLock.lock();
				syncWait.signal(); // No need to verify that the count is zero because an extra signal only hurts the 
				                   // performance but not the correctness
			} finally {
				storeSyncLock.unlock();
			}
		}
		
		return cnt;
	}
	
	public boolean markInLocalQueue() {
		return inLocalQueue.getAndSet(true);
	}
	
	public boolean clearInLocalQueue() {
		return inLocalQueue.getAndSet(false);
	}
	
	public int globalDirtyCount() {
		return globalDirtyCnt.get();
	}
	
	public int clearAndGetGlobalDirty(int count) {
		return globalDirtyCnt.addAndGet(-count);
	}
	
	public boolean inGlobalQueue() {
		return inGlobalQueue.get();
	}
	
	/**
	 * @return Returns the previous status
	 */
	public boolean markInGlobalQueue() {
		return inGlobalQueue.getAndSet(true);
	}
	
	/**
	 * @return Returns the previous status
	 */
	public boolean clearInGlobalQueue() {
		return inGlobalQueue.getAndSet(false);
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
	 * The caller thread blocks until this block is written in the local storage.
	 * @throws InterruptedException 
	 */
	public void waitUntilSynced() throws InterruptedException {
		if (localDirtyCnt.get() > 0) {
			try {
				storeSyncLock.lock();
				System.out.println("[B] Cache is full. Waiting until this block is synced to the local store: " + id);
				while (localDirtyCnt.get() > 0) {
					syncWait.await(); //FIXME: This should be the wait condition from the Cache.cacheLock.
				}
				System.out.println("[B] Block synced, now evicting: " + id);
			} finally {
				storeSyncLock.unlock();
			}
		}
	}
	
	protected abstract void getFromPrimary()  throws FileNotExistException, KawkabException, IOException;
	
	/**
	 * Helper function: Loads the block from the local or the global store. This node on which this code is executing
	 * is the primary writer of the block.
	 * 
	 * @throws FileNotExistException
	 * @throws KawkabException
	 * @throws IOException
	 */
	private void loadBlockOnPrimary() throws FileNotExistException, KawkabException, IOException {
		//Load only if it is not already loaded
		if (lastGlobalFetchTimeMs == 0) {
			try {
				dataLoadLock.lock(); // Disable loading from concurrent threads
				//Load only if it is not already loaded
				if (lastGlobalFetchTimeMs == 0) { // Prevent concurrent loads.
				
					//System.out.println("[B] On primary. Load from the LOCAL store: " + id);
					
					if (!localStoreManager.load(this)) { // Load data from the local store
						System.out.println("[B] On primary: Load from the GLOBAL store: " + id);
						globalStoreManager.load(this); // Load from the global store if failed to load from the local store
					}
					
					lastPrimaryFetchTimeMs = Long.MAX_VALUE;
					lastGlobalFetchTimeMs = Long.MAX_VALUE; // Once data is loaded on the primary, it should not expired because 
				                                        // the concurrent readers/writer read/modify the same block in the cache.
				}
			} finally {
				dataLoadLock.unlock();
			}
		}
	}
	
	/**
	 * This function loads the block's data either from the local store, global store, or from the primary node.
	 * 
	 * @throws FileNotExistException If the file is not found in any place
	 * @throws KawkabException
	 * @throws IOException
	 */
	protected void loadBlock() throws FileNotExistException, KawkabException, IOException {
		if (id.onPrimaryNode()) { // If this node is the primary writer of the file
			loadBlockOnPrimary();
			return;
		}
		
		long now = System.currentTimeMillis();
		
		if (lastGlobalFetchTimeMs < now - Constants.globalFetchExpiryTimeoutMs
				|| lastPrimaryFetchTimeMs < now - Constants.primaryFetchExpiryTimeoutMs) { // If loaded data has expired
			try {
				dataLoadLock.lock(); // Disable loading from concurrent threads
				
				now = System.currentTimeMillis();
				
				if (lastGlobalFetchTimeMs < now - Constants.globalFetchExpiryTimeoutMs) {
					//System.out.println("[B] Global fetch expired: " + id);
					/* If never fetched or the last global fetch has timed out, fetch from the global store.
					 * Otherwise, if the last remote fetch has timed out, fetch from the remote node.
					 * Otherwise, don't fetch, data is still fresh. 
					 */
					
					try {
						System.out.println("[B] Load from the global: " + id);
						
						globalStoreManager.load(this); // First try loading data from the global store
						lastGlobalFetchTimeMs = now;
						lastPrimaryFetchTimeMs = 0; //Indicates that this node has not fetched this segment from the primary node
						
						//TODO: If this block cannot be further modified, never expire the loaded data.
						
						return;
					} catch (FileNotExistException e) { //If the block is not in the global store yet
						System.out.println("[B] Not found in the global: " + id);
						lastGlobalFetchTimeMs = 0;
					}
				}
				
				// Either primary data fetch has timed out or the global fetch was timed out but the global fetch has failed
				// In both cases, we need to fetch from the primary node
				
				//System.out.println("[B] Primary fetch expired or not found from the global: " + id);
				
				try {
					//System.out.println("[B] Load from the primary: " + id);
					getFromPrimary();
					lastGlobalFetchTimeMs = (lastGlobalFetchTimeMs==0 ? now : lastGlobalFetchTimeMs);
					lastPrimaryFetchTimeMs = now;
				} catch (FileNotExistException ke) {
					// Check again from the global store because the primary may have deleted the 
					// block after copying to the global store
					System.out.println("[B] Not found on the primary, trying again from the global: " + id);
					globalStoreManager.load(this); 
					lastGlobalFetchTimeMs = now;
					lastPrimaryFetchTimeMs = 0;
				} catch (IOException ioe) {
					System.out.println("[B] Not found in the global and the primary: " + id);
					throw new KawkabException(ioe);
				}
			} finally {
				dataLoadLock.unlock();
			}
		}
	}
	
	public void setInLocalStore() {
		inLocalStore.set(true);
	}
	
	public void unsetInLocalStore() {
		inLocalStore.set(false);
	}
	
	public boolean isInLocal() {
		return inLocalStore.get();
	}
	
	public void unsetInCache() {
		inCache.set(false);
	}
	
	public boolean isInCache() {
		return inCache.get();
	}
}
