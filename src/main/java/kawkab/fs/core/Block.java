package kawkab.fs.core;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
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
 * localDirtyCnt: It keeps the count of the updates applied to the block and the updates that are persisted in the local
 * store.
 * 
 * The dirty counts are increment in the markDirty() function.
 * 
 */

public abstract class Block /*implements AutoCloseable*/ {
	protected BlockID id;
	
	private static GlobalStoreManager globalStoreManager; // Backend store such as S3
	private static LocalStoreManager localStoreManager;  // Local store such as local SSD
	
	protected static PrimaryNodeServiceClient primaryNodeService = PrimaryNodeServiceClient.instance(); // To load the block from the primary node
	
	private final Lock lock; // Block level lock
	
	private final Object localStoreSyncLock; // Lock to prevent cache eviction before syncing to the local store
	
	//private long lastGlobalFetchTimeMs = 0; //Clock time in ms when the block was last loaded from the local or the global store
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
	
	private boolean isLoaded; //If the block bytes are already loaded; used only on the primary node
	
	static { // Because we need to catch the exception
		globalStoreManager = GlobalStoreManager.instance();
		localStoreManager = LocalStoreManager.instance(); //FIXME: Handle exception properly
	}
	
	public Block(BlockID id) {
		this.id = id;
		lock = new ReentrantLock();
		
		localStoreSyncLock = new Object();
		dataLoadLock = new ReentrantLock();
		
		localDirtyCnt  = new AtomicLong(0);
		globalDirtyCnt = new AtomicInteger(0);
		inLocalQueue   = new AtomicBoolean(false);
		inGlobalQueue  = new AtomicBoolean(false);
		inLocalStore   = new AtomicBoolean(false);
		inCache        = new AtomicBoolean(true); // Initialized to true because the cache creates block objects and 
		                                          // the newly created blocks are always cached.
	}
	
	protected void reset(BlockID id) {
		this.id = id;
		localDirtyCnt.set(0);
		globalDirtyCnt.set(0);
		inLocalQueue.set(false);
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
		localDirtyCnt.incrementAndGet();
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
			synchronized (localStoreSyncLock) {
				localStoreSyncLock.notifyAll(); // Wake up the threads if they are waiting to evict this block from the cache
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
			synchronized(localStoreSyncLock) {
				while (localDirtyCnt.get() > 0 || inLocalQueue.get()) { // Wait until the block is in local queue or the block is dirty
					System.out.println("[B] Waiting until this block is synced to the local store and then evicted from the cache: " + id + ", cnt: " + 
								localDirtyCnt.get() + ", inLocalQueue="+inLocalQueue.get());
					localStoreSyncLock.wait();
				}
				System.out.println("[B] Block synced, now evicting: " + id);
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
