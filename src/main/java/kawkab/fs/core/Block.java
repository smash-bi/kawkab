package kawkab.fs.core;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.protobuf.ByteString;

import kawkab.fs.commons.Constants;
import kawkab.fs.core.exceptions.FileNotExistException;
import kawkab.fs.core.exceptions.KawkabException;
import kawkab.fs.core.services.PrimaryNodeServiceClient;

/**
 * This is a parent class for Ibmap, InodeBlock, and DataSegment classes. It provides a common interface for the subclasses
 * so that the blocks can be used in the same way.
 * 
 * globalDirtyCnt: Global dirty count is used to manage the globally persisted copy of the block. The block can only be
 * deleted from the local store if the global dirty count is zero and the block is not in the cache. The global dirty
 * count prevents too many updates to the global store.
 * 
 * localDirtyCnt: It keeps the count of the updates applied to the block and the updates that are persisted in the local
 * store.
 * 
 * The dirty counts are increment in the markDirty() function.
 * 
 */

public abstract class Block /*implements AutoCloseable*/ {
	private final BlockID id;
	
	private static GlobalStoreManager globalStoreManager; // Backend store such as S3
	private static LocalStoreManager localStoreManager;  // Local store such as local SSD
	
	protected static PrimaryNodeServiceClient primaryNodeService = PrimaryNodeServiceClient.instance(); // To load the block from the primary node
	
	private final Lock lock; // Block level lock
	
	private final Lock localStoreSyncLock; // Lock to prevent cache eviction before syncing to the local store
	private final Condition localSyncWait; // To wait until the block is synced to the local store and canbe evicted from the cache.
	
	private long lastGlobalFetchTimeMs = 0; //Clock time in ms when the block was last loaded from the local or the global store
	//private long lastPrimaryFetchTimeMs = 0; //Clock time in ms when the block was last loaded from the local or the global store
	private final Lock dataLoadLock; //Lock for loading data in memory, disabling other threads from reading the block until data is loaded
	
	private AtomicInteger globalDirtyCnt;
	private AtomicLong localDirtyCnt; // Keeps track of the number of times the block is udpated. It helps in keeping track of the 
	                        // updated bytes and flush only the updated bytes to the local/global store. 
	                        // TODO: Change this to a dirty bit. This can be achieved by an AtomicBoolean instead of
	                        // an AtomicInteger
	
	private AtomicBoolean inLocalQueue;  // The block is in a queue for local persistence
	private AtomicBoolean inGlobalQueue; // The block is in a queue for global persistence
	
	private AtomicBoolean inLocalStore; // The blocks is currently in the local store (can be in more places as well)
	private AtomicBoolean inCache;      // The block is in cache
	
	static { // Because we need to catch the exception
		try {
			globalStoreManager = GlobalStoreManager.instance();
			localStoreManager = LocalStoreManager.instance(); //FIXME: Handle exception properly
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public Block(BlockID id) {
		this.id = id;
		lock = new ReentrantLock();
		
		localStoreSyncLock = new ReentrantLock();
		localSyncWait = localStoreSyncLock.newCondition();
		
		lastGlobalFetchTimeMs = 0; //This must be initialized to zero so that the block can be loaded on a non-primary node
		//lastPrimaryFetchTimeMs = 0;
		dataLoadLock = new ReentrantLock();
		
		localDirtyCnt  = new AtomicLong(0);
		globalDirtyCnt = new AtomicInteger(0);
		inLocalQueue   = new AtomicBoolean(false);
		inGlobalQueue  = new AtomicBoolean(false);
		inLocalStore   = new AtomicBoolean(false);
		inCache        = new AtomicBoolean(true); // Initialized to true because the cache creates block objects and 
		                                          // the newly created blocks are always cached.
	}
	
	
	/**
	 * Indicates if the block is allowed be stored in the global store. Dirty bits are tested separately.
	 * @return
	 */
	protected abstract boolean shouldStoreGlobally();

	/**
	 * Load the content of the block from the given ByteBuffer
	 * 
	 * @param buffer
	 * @throws IOException
	 */
	public abstract void loadFrom(ByteBuffer buffer) throws IOException;
	
	/**
	 * Load the contents of the block from channel.
	 * 
	 * @param channel
	 * @throws IOException
	 */
	public abstract void loadFrom(ReadableByteChannel channel)  throws IOException;
	
	/**
	 * Store contents of the block to channel. This function stores only the
	 * updated bytes in the channel.
	 * 
	 * @param channel
	 * @return Number of bytes written to the channel.
	 * @throws IOException
	 */
	public abstract int storeTo(WritableByteChannel channel)  throws IOException;
	
	/**
	 * Stores the complete block in the channel.
	 * 
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
	// abstract int fromInputStream(InputStream in) throws IOException;
	
	/**
	 * Returns a ByteArrayInputStream that wraps around the byte[] containing the block in bytes. PrimaryNodeService
	 * calls this function to transfer data to the remote readers.Note that no guarantees are made about the concurrent
	 * modification of the block while the block is read from the stream.
	 */
	public abstract ByteString byteString();

	/**
	 * LocalStoreManger calls this function to set the position of the FileChannel before calling block.storeTo(channel)
	 * function. This is not a good approach because it will cause problems if multiple workers from the LocalStoreManager
	 * concurrently try to flush the same segment on disk.
	 * 
	 * @return Returns the byte offset in the segment starting from which the data will be appended.
	 */
	public abstract int appendOffsetInBlock();
	
	/**
	 * Number of bytes this block is holding in memory. This is not accurate. 
	 * @return
	 */
	public abstract int memorySizeBytes();
	
	/**
	 * @return Size of the block in bytes when the complete block is serialized. 
	 */
	public abstract int sizeWhenSerialized();
	
	@Override
	public String toString(){
		return id.name();
	}
	
	/**
	 * Increment the local and global dirty counts.
	 */
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
	
	/**
	 * @return Returns the current value of the local dirty count
	 */
	public long localDirtyCount() {
		return localDirtyCnt.get();
	}
	
	/**
	 * Decrements the local dirty count by the given count value. If the count becomes zero, the function signals any
	 * appender that is waiting for this block to be evicted from the cache.
	 * 
	 * @returns the updated value of the dirtyCounter for the local store
	 */
	public long decAndGetLocalDirty(long count) {
		long cnt = localDirtyCnt.addAndGet(-count);
		
		assert cnt >= 0;
		
		if (cnt == 0) {
			try {
				localStoreSyncLock.lock(); 
				localSyncWait.signalAll(); // Wake up the threads if they are waiting to evict this block from the cache
			} finally {
				localStoreSyncLock.unlock();
			}
			
			// There can be only one thread waiting because the cache blocks other threads before calling the 
			// waitUntilSynced() function.
		}
		
		return cnt;
	}
	
	/**
	 * Returns the current value of the global dirty count
	 * @return
	 */
	public int globalDirtyCount() {
		return globalDirtyCnt.get();
	}
	
	/**
	 * Clears the mark that the block is in a queue for persistence in the global store.
	 * @return
	 */
	public int decAndGetGlobalDirty(int count) {
		return globalDirtyCnt.addAndGet(-count);
	}
	
	public boolean inGlobalQueue() {
		return inGlobalQueue.get();
	}
	
	/**
	 * Marks that the block is in a queue to be persisted in the local store.
	 * 
	 * @return Returns true if the block was already marked to be in a local store queue
	 */
	public boolean markInLocalQueue() {
		return inLocalQueue.getAndSet(true);
	}
	
	/**
	 * Clears the mark that the block is in a queue for persistence in the local store.
	 * 
	 * @return Returns false if the marker was already clear
	 */
	public boolean clearInLocalQueue() {
		return inLocalQueue.getAndSet(false);
	}
	
	/**
	 * Marks that the block is in a queue to be persisted in the local store.
	 * 
	 * @return Returns true if the block was already marked to be in a local store queue
	 */
	public boolean markInGlobalQueue() {
		return inGlobalQueue.getAndSet(true);
	}
	
	/**
	 * Clears the mark that the block is in a queue for persistence in the local store.
	 * 
	 * @return Returns false if the marker was already clear
	 */
	public boolean clearInGlobalQueue() {
		return inGlobalQueue.getAndSet(false);
	}
	
	public abstract boolean evictLocallyOnMemoryEviction();
	
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
	public void cleanup() throws IOException {  }
	
	/**
	 * The caller thread blocks until this block is flushed in the local storage and the local dirty count becomes zero.
	 * 
	 * @throws InterruptedException 
	 */
	public void waitUntilSynced() throws InterruptedException {
		if (localDirtyCnt.get() > 0) {
			try {
				localStoreSyncLock.lock();
				
				while (localDirtyCnt.get() > 0 || inLocalQueue.get()) { // Wait until the block is in local queue or the block is dirty
					System.out.println("[B] Waiting to evict until this block is synced to the local store: " + id + ", cnt: " + localDirtyCnt.get() + ", inLocalQueue="+inLocalQueue.get());
					localSyncWait.await();
				}
				System.out.println("[B] Block synced, now evicting: " + id);
			} finally {
				localStoreSyncLock.unlock();
			}
		}
	}
	
	/**
	 * Helper function to load the block from the global store
	 * @throws FileNotExistException
	 * @throws KawkabException
	 */
	protected void loadFromGlobal() throws FileNotExistException, KawkabException {
		globalStoreManager.load(this);
	}
	
	/**
	 * This function loads the block's data either from the local store, global store, or from the primary node.
	 * 
	 * The caller blocks until data loading is complete.
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
		
		// The following code is run only on a non-primary node with respect to this block
		
		/* If never fetched or the last global-fetch has timed out, fetch from the global store.
		 * Otherwise, if the last primary-fetch has timed out, fetch from the primary node.
		 * Otherwise, don't fetch, data is still fresh. 
		 */
		
		long now = System.currentTimeMillis();
		
		if (lastGlobalFetchTimeMs < now - Constants.globalFetchExpiryTimeoutMs) { // If the last fetch from the global store has expired
			try {
				dataLoadLock.lock(); // Prevent loading from concurrent readers
				
				now = System.currentTimeMillis();
				
				if (lastGlobalFetchTimeMs < now - Constants.globalFetchExpiryTimeoutMs) { // If the last fetch from the global store has expired
					try {
						System.out.println("[B] Load from the global: " + id);
						
						globalStoreManager.load(this); // First try loading data from the global store
						lastGlobalFetchTimeMs = now; // Never expire data fetched from the global store. 
																// InodeBlocks are updated to the global store (not treating as an append only system).
						//lastPrimaryFetchTimeMs = 0; //Indicates that this node has not fetched this block from the primary node
						
						return;
						//TODO: If this block cannot be further modified, never expire the loaded data. For example, if it was the last segment of the block.
					} catch (FileNotExistException e) { //If the block is not in the global store yet
						System.out.println("[B] Not found in the global: " + id);
						lastGlobalFetchTimeMs = 0; // Failed to fetch from the global store
					}
				
					System.out.println("[B] Primary fetch expired or not found from the global: " + id);
					
					try {
						System.out.println("[B] Loading from the primary: " + id);
						loadBlockFromPrimary(); // Fetch from the primary node
						//lastPrimaryFetchTimeMs = now;
						if (lastGlobalFetchTimeMs == 0) // Set to now if the global fetch has failed
							lastGlobalFetchTimeMs = now;
					} catch (FileNotExistException ke) { // If the file is not on the primary node, check again from the global store
						// Check again from the global store because the primary may have deleted the 
						// block after copying to the global store
						System.out.println("[B] Not found on the primary, trying again from the global: " + id);
						globalStoreManager.load(this); 
						lastGlobalFetchTimeMs = now;
						//lastPrimaryFetchTimeMs = 0;
					} catch (IOException ioe) {
						System.out.println("[B] Not found in the global and the primary: " + id);
						throw new KawkabException(ioe);
					}
				}
			} finally {
				dataLoadLock.unlock();
			}
		}
	}
	
	/**
	 * Load block on the current non-primary node
	 * 
	 * @throws FileNotExistException
	 * @throws KawkabException
	 * @throws IOException
	 */
	protected abstract void loadBlockNonPrimary() throws FileNotExistException, KawkabException, IOException;
	
	/**
	 * Helper function: Loads the block from the local or the global store. This code runs only on the primary node
	 * of this block.
	 * 
	 * @throws FileNotExistException
	 * @throws KawkabException
	 * @throws IOException
	 */
	private void loadBlockOnPrimary() throws FileNotExistException, KawkabException, IOException {
		if (!id.onPrimaryNode()) { //FIXME: Do we need to check again? First time this is checked in the loadBlock().
			throw new KawkabException("Unexpected execution path. This node is not the primary node of this block: " + 
																		id() + ", primary node: " + id.primaryNodeID());
		}
		
		//Load only if the block is not already loaded
		if (lastGlobalFetchTimeMs == 0) {
			try {
				dataLoadLock.lock(); // Disable loading from concurrent threads
				//Load only if it is not already loaded
				if (lastGlobalFetchTimeMs == 0) { // Prevent subsequent loads from other threads
				
					//System.out.println("[B] On primary. Load from the LOCAL store: " + id);
					
					if (!localStoreManager.load(this)) { // Load data from the local store
						System.out.println("[B] On primary: Load from the GLOBAL store: " + id);
						globalStoreManager.load(this); // Load from the global store if failed to load from the local store
					}
					
					//lastPrimaryFetchTimeMs = Long.MAX_VALUE;
					lastGlobalFetchTimeMs = Long.MAX_VALUE; // Once data is loaded on the primary, it should not expired because 
				                                        // the concurrent readers/writer read/modify the same block in the cache.
				}
			} finally {
				dataLoadLock.unlock();
			}
		}
	}
	
	/**
	 * Helper function to load the content of the block from the primary node.
	 * 
	 * @throws FileNotExistException
	 * @throws KawkabException
	 * @throws IOException
	 */
	protected abstract void loadBlockFromPrimary()  throws FileNotExistException, KawkabException, IOException;
	
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
