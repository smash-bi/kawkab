package kawkab.fs.core;

import com.google.protobuf.ByteString;
import kawkab.fs.commons.Configuration;
import kawkab.fs.core.exceptions.FileNotExistException;
import kawkab.fs.core.exceptions.KawkabException;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.StandardOpenOption;

public final class InodesBlock extends Block {
	private static boolean bootstraped; //Not saved persistently
	private Inode[] inodes; //Should be initialized in the bootstrap function only.
	private long lastFetchTimeMs; 	// Clock time in ms when the block was last loaded. This must be initialized 
									// to zero when the block is first created in memory.

	private static Clock clock = Clock.instance();
	private long lastGlobalStoreTimeMs;
	private int globalStoreTimeGapMs = 3000;
	//private int version; //Inodes-block's current version number.
	
	private static Configuration conf = Configuration.instance();

	/*
	 * The access modifier of the constructor is "default" so that it can be packaged as a library. Clients
	 * are not supposed to extend or instantiate this class.
	 */
	InodesBlock(InodesBlockID id){
		super(id);
		
		inodes = new Inode[conf.inodesPerBlock];
		int blockIndex = id.blockIndex();
		for (int j=0; j<conf.inodesPerBlock; j++) {
			long inumber = blockIndex*conf.inodesPerBlock + j;
			initInode(inumber, 0); //Initially set to zero to catch any errors. The recordSize must be at least 1 in the working system
		}
	}
	
	protected void initInode(long inumber, int recordSize) {
		int inumberIdx = inodeIdxFromInumber(inumber);
		inodes[inumberIdx] = new Inode(inumber, recordSize);
		markLocalDirty();
	}
	
	/**
	 * @param inumber 
	 * @return Returns the Inode corresponding to the inumber
	 */
	public Inode getInode(long inumber){
		return inodes[inodeIdxFromInumber(inumber)];
	}
	
	/**
	 * Returns the current size of the file, in bytes, that is associated with the given inumber.
	 * @param inumber the inode number associated with the file. The caller of this function must
	 *         ensure that the inumber belongs to this inodesBlock.
	 *         
	 * @return
	 */
	public long fileSize(long inumber){
		int inodeNumber = inodeIdxFromInumber(inumber);
		return inodes[inodeNumber].fileSize();
	}
	
	@Override
	public boolean shouldStoreGlobally() {
		//FIXME: Potentially this block will never be stored globally. This may happen if the block is updated
		// but not stored globally. After that the block is never updated.
		long now = clock.currentTime();
		if ((now - lastGlobalStoreTimeMs) < globalStoreTimeGapMs) {
			System.out.printf("[IB] Not storing %s globally. Timeout=%d, now=%d, now-lastStore=%d.\n",
					id, lastGlobalStoreTimeMs, now, now-lastGlobalStoreTimeMs);
			return false;
		}
		
		lastGlobalStoreTimeMs = now;

		System.out.printf("[IB] >>>> Storing IB %s globally.\n", id);
		return true;
		//return false; //FIXME: Disabled inodesBlocks transfer to the GlobalStore
	}
	
	@Override
	public boolean evictLocallyOnMemoryEviction() {
		return false;
	}
	
	@Override
	public int loadFrom(ByteBuffer buffer) throws IOException {
		int bytesRead = 0;

		for(int i=0; i<conf.inodesPerBlock; i++) {
			bytesRead += inodes[i].loadFrom(buffer);
		}

		return bytesRead;
	}
	
	@Override
	public int loadFromFile() throws IOException {
		try (RandomAccessFile file = new RandomAccessFile(id.localPath(), "r");
				SeekableByteChannel channel = file.getChannel()) {
			channel.position(0);
			// System.out.println("Load: "+block.localPath() + ": " + channel.position());
			return loadFrom(channel);
		}
	}
	
	@Override
	public int loadFrom(ReadableByteChannel channel) throws IOException {
		int bytesRead = 0;
		//inodes = new Inode[Constants.inodesPerBlock];
		for(int i=0; i<conf.inodesPerBlock; i++){
			//inodes[i] = new Inode(0);
			bytesRead += inodes[i].loadFrom(channel);
		}

		return bytesRead;
	}
	
	@Override
	public int storeToFile() throws IOException {
		FileChannel channel = FileChannel.open(new File(id().localPath()).toPath(), StandardOpenOption.WRITE);
		return storeTo(channel);
	}
	
	@Override
	public int storeTo(FileChannel channel) throws IOException {
		//System.out.printf("[IB] Store %s to channel\n", id);
		int bytesWritten = 0;
		channel.position(0);
		for(Inode inode : inodes) {
			bytesWritten += inode.storeTo(channel);
		}
		
		return bytesWritten;
	}
	
	@Override 
	public ByteString byteString() {
		//TODO: This function takes extra memory to serialize inodes in an input stream. We need an alternate
		//method for this purpose.
		ByteBuffer buffer = ByteBuffer.allocate(inodes.length * conf.inodeSizeBytes);
		for(Inode inode : inodes) {
			inode.storeTo(buffer);
		}
		buffer.flip();
		return ByteString.copyFrom(buffer.array());
	}
	
	private void loadBlockFromPrimary()  throws FileNotExistException, KawkabException, IOException {
		primaryNodeService.getInodesBlock((InodesBlockID)id(), this);
	}
	
	@Override
	protected void onMemoryEviction() {
		// Do nothing
	}
	
	@Override
	protected synchronized void loadBlockOnNonPrimary() throws FileNotExistException, KawkabException, IOException {
		/* If never fetched or the last global-fetch has timed out, fetch from the global store.
		 * Otherwise, if the last primary-fetch has timed out, fetch from the primary node.
		 * Otherwise, don't fetch, data is still fresh. 
		 */
		
		long now = System.currentTimeMillis();

		System.out.printf("[IB] loadOnNonPrimary: lastFetchMS=%d, now-timeout=%d\n", lastFetchTimeMs, now-conf.inodesBlockFetchExpiryTimeoutMs);
		
		if (lastFetchTimeMs < now - conf.inodesBlockFetchExpiryTimeoutMs) { // If the last fetch from the global store has expired
			/*try {
				System.out.println("[IB] Load from the global: " + id());

				loadFromGlobal(0, conf.inodesBlockSizeBytes); // First try loading data from the global store
				return;

				//TODO: If this block cannot be further modified, never expire the loaded data. For example, if it was the last segment of the block.
			} catch (FileNotExistException e) { //If the block is not in the global store yet
				System.out.println("[B] Not found in the global: " + id());
				lastFetchTimeMs = 0; // Failed to fetch from the global store
			}

			System.out.println("[B] Primary fetch expired or not found from the global: " + id());*/

			try {
				System.out.println("[B] Loading from the primary: " + id());
				loadBlockFromPrimary(); // Fetch data from the primary node
				//lastPrimaryFetchTimeMs = now;
				if (lastFetchTimeMs == 0) // Set to now if the global fetch has failed
					lastFetchTimeMs = now;
			} catch (FileNotExistException ke) { // If the file is not on the primary node, check again from the global store
				// Check again from the global store because the primary may have deleted the
				// block after copying to the global store
				System.out.println("[B] Not found on the primary, trying again from the global: " + id());
				loadFromGlobal(0, conf.inodesBlockSizeBytes);
				lastFetchTimeMs = now;
				//lastPrimaryFetchTimeMs = 0;
			} catch (IOException ioe) {
				System.out.println("[B] Not found in the global and the primary: " + id());
				throw new KawkabException(ioe);
			}
		}
	}

	@Override
	public int sizeWhenSerialized() {
		return conf.inodesBlockSizeBytes;
	}
	
	/*
	 * (non-Javadoc)
	 * @see kawkab.fs.core.Block#toBuffer(java.nio.ByteBuffer)
	 */
	/*@Override
	void toBuffer(ByteBuffer buffer) throws InsufficientResourcesException{
		//FIXME: Do we need to acquire the lock here??? We should not! Otherwise, we will have a high performance hit.
		
		int blockSize = Constants.inodesBlockSizeBytes;
		if (buffer.capacity() < blockSize)
			throw new InsufficientResourcesException(String.format("Buffer capacity is less than "
						+ "required: Capacity = %d bytes, required = %d bytes.",buffer.capacity(),blockSize));

		for(Inode inode : inodes) {
			inode.toBuffer(buffer);
		}
	}*/
	
	/**
	 * Creates inodeBlocks on local disk belonging to this machine.
	 * @throws IOException
	 * @throws InterruptedException 
	 * @throws KawkabException 
	 */
	static void bootstrap() throws IOException, InterruptedException, KawkabException{
		if (bootstraped)
			return;
		
		System.out.println("Bootstrap InodesBlocks");
		
		File folder = new File(conf.inodeBlocksPath);
		if (!folder.exists()){
			System.out.println("  Creating folder: " + folder.getAbsolutePath());
			folder.mkdirs();
		}
		
		int count=0;
		LocalStoreManager storage = LocalStoreManager.instance();
		//Cache cache = Cache.instance();
		int rangeStart = conf.inodeBlocksRangeStart;
		int rangeEnd = rangeStart + conf.inodeBlocksPerMachine;
		for(int i=rangeStart; i<rangeEnd; i++){
			InodesBlockID id = new InodesBlockID(i);
			File file = new File(id.localPath());
			if (!file.exists()) {
				storage.createBlock(id);
				Block block = id.newBlock();
				block.storeToFile();
				count++;
				
				/*try {
					Block block = cache.acquireBlock(id, true); // Create a new block in the local store
					block.markLocalDirty(); // Mark the new block as dirty to write the initialized inodes
					count++;
				} finally {
					cache.releaseBlock(id);
				}*/
			}
		}
		
		System.out.println("Created inodeBlock files: " + count);
		bootstraped = true;
	}
	
	static void shutdown(){
		System.out.println("Closing InodesBlock");
	}
	
	static int blockIndexFromInumber(long inumber) {
		return (int)(inumber / conf.inodesPerBlock);
	}
	
	private int inodeIdxFromInumber(long inumber){
		return (int)(inumber % conf.inodesPerBlock);
	}
}
