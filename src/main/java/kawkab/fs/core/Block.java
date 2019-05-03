package kawkab.fs.core;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.protobuf.ByteString;

import kawkab.fs.core.exceptions.FileNotExistException;
import kawkab.fs.core.exceptions.KawkabException;
import kawkab.fs.core.services.grpc.PrimaryNodeServiceClient;

/**
 * This is a parent class for Ibmap, InodeBlock, and DataSegment classes. It provides a common interface for the subclasses
 * so that the blocks can be used in the same way.
 * 
 * globalDirtyCnt: Global dirty count is used to manage the globally persisted copy of the block. The block can only be
 * deleted from the local store if the global dirty count is zero and the block is not in the cache. The global dirty
 * count prevents too many updates to the global store.
 * 
 * isLocalDirty: It keeps the count of the updates applied to the block and the updates that are persisted in the local
 * store.
 * 
 * The dirty counts are increment in the markDirty() function.
 * 
 */

public abstract class Block extends AbstractTransferItem {
	protected BlockID id;
	
	private final static GlobalStoreManager globalStoreManager = GlobalStoreManager.instance(); // Backend store such as S3
	private final static LocalStoreManager localStoreManager = LocalStoreManager.instance(); //FIXME: Handle exception properly;  // Local store such as local SSD
	protected final static PrimaryNodeServiceClient primaryNodeService = PrimaryNodeServiceClient.instance(); // To load the block from the primary node
	
	private final Object localStoreSyncLock; // Lock to prevent cache eviction before syncing to the local store
	
	private final Lock dataLoadLock; //Lock for loading data in memory, disabling other threads from reading the block until data is loaded
	
	private AtomicInteger globalDirtyCnt;
	private volatile boolean isLocalDirty; // Keeps track of the number of times the block is udpated. It helps in keeping track of the
	                        // updated bytes and flush only the updated bytes to the local/global store. 
	                        // TODO: Change this to a dirty bit. This can be achieved by an AtomicBoolean instead of
	                        // an AtomicInteger
	
	//private volatile boolean inLocalQueue;  // The block is in a queue for local persistence
	private AtomicBoolean inGlobalQueue; // The block is in a queue for global persistence
	private AtomicBoolean inLocalStore; // The blocks is currently in the local store (can be in more places as well)
	private AtomicBoolean inCache;      // The block is in cache 
	
	private boolean isLoaded; //If the block bytes are already loaded; used only on the primary node
	
	public long inQCount = 0; //For debug purposes
	public long inTries = 0; //For debug purposes
	
	public Block(BlockID id) {
		this.id = id;
		
		localStoreSyncLock = new Object();
		dataLoadLock = new ReentrantLock();
		
		isLocalDirty = false;
		globalDirtyCnt = new AtomicInteger(0);
		//inLocalQueue   = false;
		inGlobalQueue  = new AtomicBoolean(false);
		inLocalStore   = new AtomicBoolean(false);
		inCache        = new AtomicBoolean(true); // Initialized to true because the cache creates block objects and 
		                                          // the newly created blocks are always cached.
	}
	
	protected void reset(BlockID id) {
		this.id = id;
		isLocalDirty = false;
		globalDirtyCnt.set(0);
		//inLocalQueue = false;
		inGlobalQueue.set(false);
		inLocalStore.set(false);
		inCache.set(false);
	}
	
	/**
	 * Indicates if the block is allowed be stored in the global store. Dirty bits are tested separately.
	 * @return
	 */
	protected abstract boolean shouldStoreGlobally();

	/**
	 * Load the content of the block from the local file
	 *
	 * @return Number of bytes loaded from the file
	 * @throws IOException
	 */
	public abstract int loadFromFile() throws IOException;
	
	/**
	 * Load the content of the block from the given ByteBuffer
	 * 
	 * @param buffer
	 *
	 * @return Number of bytes read from the buffer
	 * @throws IOException
	 */
	public abstract int loadFrom(ByteBuffer buffer) throws IOException;
	
	/**
	 * Load the contents of the block from channel.
	 * 
	 * @param channel
	 *
	 * @return Number of bytes read from the channel
	 * @throws IOException
	 */
	public abstract int loadFrom(ReadableByteChannel channel)  throws IOException;
	
	/**
	 * Store contents of the block to channel. This function stores only the
	 * updated bytes in the channel.
	 * 
	 * @param channel
	 * @return Number of bytes written to the channel.
	 * @throws IOException
	 */
	abstract int storeTo(WritableByteChannel channel)  throws IOException;
	
	/**
	 * Stores the block in a file.
	 * @return Number of bytes written in the file.
	 */
	abstract int storeToFile() throws IOException;
	
	/**
	 * Stores the complete block in the channel.
	 * 
	 * @param channel
	 * @return Number of bytes written to the channel.
	 * @throws IOException
	 */
	//public abstract int storeFullTo(WritableByteChannel channel) throws IOException;
	
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
		return id.toString();
	}
	
	/**
	 * Increment the local dirty counts.
	 */
	public void markLocalDirty() {
		isLocalDirty = true;
	}
	
	/**
	 * Increment the global dirty counts.
	 */
	public void markGlobalDirty() {
		globalDirtyCnt.incrementAndGet();
	}
	
	/**
	 * @return Returns whether this block has been modified until now or not.
	 */
	public boolean isLocalDirty(){
		return isLocalDirty;
	}
	
	/**
	 * @return Returns the current value of the local dirty count
	 */
	/*public long localDirtyCount() {
		return isLocalDirty.get();
	}*/
	
	public boolean getAndClearLocalDirty() {
		boolean isDirty = isLocalDirty;
		isLocalDirty = false;
		return isDirty;
	}
	
	public void notifySyncComplete() {
		synchronized (localStoreSyncLock) {
			localStoreSyncLock.notifyAll(); // Wake up the threads if they are waiting to evict this block from the cache
		}
	}
	
	/**
	 * Returns the current value of the global dirty count
	 * @return
	 */
	public int globalDirtyCount() {
		return globalDirtyCnt.get();
	}
	
	public int decAndGetGlobalDirty(int count) {
		return globalDirtyCnt.addAndGet(-count);
	}
	
	public boolean inGlobalQueue() {
		return inGlobalQueue.get();
	}
	
	/**
	 * Marks that the block is in a queue to be persisted in the local store.
	 * 
	 * @return Returns true if the block was already marked to be in the local queue
	 */
	/*public boolean markInLocalQueue() {
		*//*if (inLocalQueue.get())
			return true;
		return inLocalQueue.getAndSet(true);*//*
		
		if (inLocalQueue)
			return true;
		inLocalQueue = true;
		return false;
	}*/
	
	/**
	 * Clears the mark that the block is in a queue for persistence in the local store.
	 * 
	 * @return Returns false if the marker was already clear
	 */
	/*public void clearInLocalQueue() {
		if (!inLocalQueue)
			return;
		
		inLocalQueue = false;
		
		*//*if (!inLocalQueue.get())
			return false;
		return inLocalQueue.getAndSet(false);*//*
	}*/
	
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
	
	public BlockID id(){
		return id;
	}
	
	/**
	 * The caller thread blocks until this block is flushed in the local storage and the local dirty count becomes zero.
	 * 
	 * @throws InterruptedException 
	 */
	public void waitUntilSynced() throws InterruptedException {
		if (isLocalDirty || inTransferQueue()) {
			synchronized(localStoreSyncLock) {
				while (isLocalDirty || inTransferQueue()) { // It may happen that the block's dirty count is zero but the block is also in the queue. See (1) at the end of this file.
					//System.out.println("[B] Waiting until this block is synced to the local store and then evicted from the cache: " + id + ", isLocalDirty: " +
					//			isLocalDirty + ", inLocalQueue="+inTransferQueue());
					localStoreSyncLock.wait();
				}
				//System.out.println("[B] Block synced, now evicting: " + id);
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
	public void loadBlock() throws FileNotExistException, KawkabException, IOException {
		if (id.onPrimaryNode()) { // If this node is the primary writer of the file
			loadBlockOnPrimary();
			return;
		}
		
		loadBlockOnNonPrimary();
	}
	
	/**
	 * Load block on the current non-primary node
	 * 
	 * @throws FileNotExistException
	 * @throws KawkabException
	 * @throws IOException
	 */
	protected abstract void loadBlockOnNonPrimary() throws FileNotExistException, KawkabException, IOException;
	
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
		if (!isLoaded) {
			try {
				dataLoadLock.lock(); // Disable loading from concurrent threads
				//Load only if it is not already loaded
				if (!isLoaded) { // Prevent subsequent loads from other threads
				
					//System.out.println("[B] On primary. Load from the LOCAL store: " + id);
					
					if (!localStoreManager.load(this)) { // Load data from the local store
						System.out.println("[B] On primary: Loading from the GLOBAL STORE: " + id);
						loadFromGlobal(); // Load from the global store if failed to load from the local store
					}
					
					//lastPrimaryFetchTimeMs = Long.MAX_VALUE;
					isLoaded = true; //Once data is loaded on the primary, it should not expired because 
				                     // the concurrent readers/writer read/modify the same block in the cache.
				}
				isLoaded = true;
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
	
	void setInLocalStore() {
		inLocalStore.set(true);
	}
	
	void unsetInLocalStore() {
		inLocalStore.set(false);
	}
	
	boolean isInLocal() {
		return inLocalStore.get();
	}
	
	void unsetInCache() {
		inCache.set(false);
	}
	
	boolean isInCache() {
		return inCache.get();
	}
	
	abstract void onMemoryEviction();
}

/**
 * (1)	It may happen that a block's dirty count is zero and the block is in the queue:
 * 		- Appender writes the block B and increments dirty count d
 * 		- Appender puts the block in queue after marking that the block is in the queue
 * 		- LocalStore takes the block from the queue and marks the block as not in queue
 * 		- LocalStore thread sleeps
 * 		- Appender updates the block, increments dirty count d, and puts the block again in the queue after marking inQueue bit
 * 		- LocalStore wakes and reads the dirty count to be two, writes data to disk, and decrements the dirty count by 2, making it zero
 * 		- As the dirty count is zero, LocalStore wakes any thread wiating on localStoreSyncLock
 * 		- A thread wakes and finds that the block is still in queue. Therefore, it cannot be garbage collected and so sleeps again
 * 		- LocalStore takes the same block again from the queue, finds the dirty cont to zero, so wakes the threads on
 * 		  localStoreSyncQueue and does not persist data on disk
 */