package kawkab.fs.core;

import kawkab.fs.core.exceptions.FileNotExistException;
import kawkab.fs.core.exceptions.KawkabException;
import kawkab.fs.core.services.thrift.PrimaryNodeServiceClient;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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
	
	//private AtomicInteger globalDirtyCnt;
	private AtomicInteger localDirtyCnt; // Keeps track of the number of times the block is udpated. It helps in keeping track of the
	                        // updated bytes and flush only the updated bytes to the local/global store. 
	                        // TODO: Change this to a dirty bit. This can be achieved by an AtomicBoolean instead of
	                        // an AtomicInteger
	
	//private volatile boolean inLocalQueue;  // The block is in a queue for local persistence
	//private AtomicBoolean inGlobalQueue; // The block is in a queue for global persistence
	private AtomicBoolean inLocalStore; // The blocks is currently in the local store (can be in more places as well)
	//private AtomicBoolean inCache;      // The block is in cache
	
	private boolean isLoaded; //If the block bytes are already loaded; used only on the primary node

	protected boolean isOnPrimary;

	//public long inQCount = 0; //For debug purposes
	//public long inTries = 0; //For debug purposes
	
	public Block(BlockID id) {
		try {
			this.id = id;
			isOnPrimary = id != null && id.onPrimaryNode();
			localDirtyCnt = new AtomicInteger(0);
			//globalDirtyCnt = new AtomicInteger(0);
			//inGlobalQueue  = new AtomicBoolean(false);
			inLocalStore = new AtomicBoolean(false);
			//inCache        = new AtomicBoolean(true); // Initialized to true because the cache creates block objects and
			// the newly created blocks are always cached.
			isLoaded = false;
		} catch (Exception | AssertionError e) {
			e.printStackTrace();
		}
	}
	
	protected void reset(BlockID id) {
		assert (id==null && this.id!=null) || (id!= null && this.id==null) : String.format("id=%s, this.id=%s",id, this.id);

		this.id = id;
		localDirtyCnt.set(0);
		//globalDirtyCnt.set(0);
		//inGlobalQueue.set(false);
		inLocalStore.set(false);
		//inCache.set(false);
		isLoaded = false;
		isOnPrimary = id != null && id.onPrimaryNode();
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
	 * dirty bytes in the channel.
	 * 
	 * @param channel
	 * @return Number of bytes written to the channel.
	 * @throws IOException
	 */
	protected abstract int storeTo(FileChannel channel)  throws IOException;
	
	/**
	 * Stores the block in a file.
	 * @return Number of bytes written in the file.
	 */
	protected abstract int storeToFile() throws IOException;

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
	/*public void markGlobalDirty() {
		globalDirtyCnt.incrementAndGet();
	}*/
	
	/**
	 * @return Returns whether this block has been modified until now or not.
	 */
	public boolean isLocalDirty(){
		return localDirtyCnt.get() > 0;
	}

	/**
	 * @return Returns the current value of the local dirty count
	 */
	public int localDirtyCount() {
		return localDirtyCnt.get();
	}

	public int decAndGetLocalDirty(int delta) {
		int cnt = localDirtyCnt.addAndGet(-delta);

		assert cnt >= 0;

		return cnt;

		/*boolean isDirty = localDirtyCnt;
		localDirtyCnt = false;
		return isDirty;*/
	}
	
	public void notifyLocalSyncComplete() {
		synchronized (this) {
			notifyAll(); // Wake up the threads if they are waiting to evict this block from the cache
		}
	}
	
	/**
	 * Returns the current value of the global dirty count
	 * @return
	 */
	/*public int globalDirtyCount() {
		return globalDirtyCnt.get();
	}*/
	
	/*public int decAndGetGlobalDirty(int count) {
		return globalDirtyCnt.addAndGet(-count);
	}*/
	
	/*public boolean inGlobalQueue() {
		return inGlobalQueue.get();
	}*/
	
	/**
	 * Marks that the block is in a queue to be persisted in the local store.
	 * 
	 * @return Returns true if the block was already marked to be in a local store queue
	 */
	/*public boolean markInGlobalQueue() {
		return inGlobalQueue.getAndSet(true);
	}*/
	
	/**
	 * Clears the mark that the block is in a queue for persistence in the local store.
	 * 
	 * @return Returns false if the marker was already clear
	 */
	/*public boolean clearInGlobalQueue() {
		return inGlobalQueue.getAndSet(false);
	}*/
	
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
		if (localDirtyCnt.get() > 0 || inTransferQueue()) {
			synchronized(this) {
				// It may happen that the block's dirty count is zero but the block is also in the queue. See (1) at the end of this file.
				while (localDirtyCnt.get() > 0 || inTransferQueue()) {
					//System.out.println("[B] swait: " + id + ", dty: " +
					//			isLocalDirty + ", inLQ="+inTransferQueue());
					wait();
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
	protected void loadFromGlobal(int offset, int length) throws FileNotExistException, IOException {
		//System.out.printf("[B] Loading %s from GS\n",id);

		globalStoreManager.bulkLoad(this, offset, length);
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
	public void loadBlock(boolean loadFromPrimary) throws FileNotExistException, IOException {
		if (isOnPrimary) { // If this node is the primary writer of the file
			if (!isLoaded)
				loadBlockOnPrimary();
			return;
		}
		
		loadBlockOnNonPrimary(loadFromPrimary);
	}
	
	/**
	 * Load block on the current non-primary node
	 *
	 * If this node is the primary node of this block, the block is first attempted to be loaded from the local store.
	 * If this node is a non-primary node, the loadFromPrimary parameter indicates whether to load only from the primary
	 * or follow another policy, e.g., first try to load from the global store.
	 *
	 * @param loadFromPrimary Whether to load the block only from the primary if this node is a non-primary node. This
	 *                        flag is checked only on a non-primary node.
	 *
	 * @throws FileNotExistException
	 * @throws KawkabException
	 * @throws IOException
	 */
	protected abstract void loadBlockOnNonPrimary(boolean loadFromPrimary) throws FileNotExistException, IOException;
	
	/**
	 * Helper function: Loads the block from the local or the global store. This code runs only on the primary node
	 * of this block.
	 * 
	 * @throws FileNotExistException
	 * @throws KawkabException
	 * @throws IOException
	 */
	private void loadBlockOnPrimary() throws FileNotExistException, IOException {
		if (!isOnPrimary) { //FIXME: Do we need to check again? First time this is checked in the loadBlock().
			assert false : "Unexpected execution path. This node is not the primary node of this block: " +
					id() + ", primary node: " + id.primaryNodeID();
		}

		//Load only if the block is not already loaded
		if (!isLoaded) {
			//dataLoadLock.lock(); // Disable loading from concurrent threads
			synchronized (this) {
				//Load only if it is not already loaded
				if (!isLoaded) { // Prevent subsequent loads from other threads
					//System.out.println(" [B] **** LOAD BLOCK ON PRIMARY: " + id);
					//System.out.println("[B] On primary. Load from the LOCAL store: " + id);

					if (!localStoreManager.load(this)) { // Load data from the local store
						System.out.println("[B] On primary: Loading from the GLOBAL STORE: " + id);
						loadFromGlobal(0, sizeWhenSerialized()); // Load from the global store if failed to load from the local store
					}

					isLoaded = true; //Once data is loaded on the primary, it should not expired because
					// the concurrent readers/writer read/modify the same block in the cache.
				}
			}
		}
	}

	/*void setInLocalStore() {
		inLocalStore.set(true);
	}
	
	void unsetInLocalStore() {
		inLocalStore.set(false);
	}
	
	boolean isInLocal() {
		return inLocalStore.get();
	}*/
	
	/*void unsetInCache() {
		inCache.set(false);
	}
	
	boolean isInCache() {
		return inCache.get();
	}*/
	
	protected void setIsLoaded() {
		//System.out.printf("[B] Setting %s is loaded\n",id);
		isLoaded = true;
	}

	/*public boolean isLoaded() {
		return isLoaded;
	}*/
}

/*
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