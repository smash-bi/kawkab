package kawkab.fs.core;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;

import com.google.protobuf.ByteString;

import io.grpc.netty.shaded.io.netty.buffer.ByteBuf;
import kawkab.fs.commons.Commons;
import kawkab.fs.commons.Configuration;
import kawkab.fs.core.exceptions.FileNotExistException;
import kawkab.fs.core.exceptions.KawkabException;

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
	
	private boolean opened = false;
	RandomAccessFile rwFile;
	SeekableByteChannel channel;
	
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
			initInode(inumber);
		}
	}
	
	protected void initInode(long inumber) {
		int inumberIdx = inodeIdxFromInumber(inumber);
		inodes[inumberIdx] = new Inode(inumber);
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
		long now = clock.currentTime();
		if ((now - lastGlobalStoreTimeMs) < globalStoreTimeGapMs)
			return false;
		
		lastGlobalStoreTimeMs = now;
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

		for(int i=0; i<conf.inodesPerBlock; i++){
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
	
	private ByteBuffer buffer = ByteBuffer.allocate(conf.inodeSizeBytes);
	
	@Override
	public int storeToFile() throws IOException {
		if (!opened) {
			synchronized (this) {
				if (!opened) {
					rwFile = new RandomAccessFile(id.localPath(), "rw");
					channel = rwFile.getChannel();
					opened = true;
				}
			}
		}
		
		channel.position(0);
		//System.out.println("Store: "+id() + ": " + channel.position());
		int bytesWritten = 0;
		for(Inode inode : inodes) {
			buffer.clear();
			inode.storeTo(buffer);
			buffer.rewind();
			bytesWritten += Commons.writeTo(channel, buffer);
		}
		return bytesWritten;
	}
	
	@Override
	public int storeTo(WritableByteChannel channel) throws IOException {
		int bytesWritten = 0;
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
	
	@Override
	protected void loadBlockFromPrimary()  throws FileNotExistException, KawkabException, IOException {
		primaryNodeService.getInodesBlock((InodesBlockID)id(), this);
	}
	
	@Override
	void onMemoryEviction() {
		if (opened) {
			synchronized (this) {
				if (opened) {
					opened = false;
					try {
						rwFile.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
					
					try {
						channel.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		}
	}
	
	@Override
	protected synchronized void loadBlockOnNonPrimary() throws FileNotExistException, KawkabException, IOException {
		/* If never fetched or the last global-fetch has timed out, fetch from the global store.
		 * Otherwise, if the last primary-fetch has timed out, fetch from the primary node.
		 * Otherwise, don't fetch, data is still fresh. 
		 */
		
		long now = System.currentTimeMillis();
		
		if (lastFetchTimeMs < now - conf.inodesBlockFetchExpiryTimeoutMs) { // If the last data-fetch-time exceeds the time limit
				
			now = System.currentTimeMillis();
			
			if (lastFetchTimeMs < now - conf.inodesBlockFetchExpiryTimeoutMs) { // If the last fetch from the global store has expired
				try {
					// System.out.println("[IB] Load from the global: " + id());
					
					loadFromGlobal(); // First try loading data from the global store
					return;
					
					//TODO: If this block cannot be further modified, never expire the loaded data. For example, if it was the last segment of the block.
				} catch (FileNotExistException e) { //If the block is not in the global store yet
					System.out.println("[B] Not found in the global: " + id());
					lastFetchTimeMs = 0; // Failed to fetch from the global store
				}
			
				System.out.println("[B] Primary fetch expired or not found from the global: " + id());
				
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
					loadFromGlobal(); 
					lastFetchTimeMs = now;
					//lastPrimaryFetchTimeMs = 0;
				} catch (IOException ioe) {
					System.out.println("[B] Not found in the global and the primary: " + id());
					throw new KawkabException(ioe);
				}
			}
		}
	}

	@Override
	public int appendOffsetInBlock() {
		return 0;
	}
	
	@Override
	public int memorySizeBytes() {
		return conf.inodesBlockSizeBytes + 8; //FIXME: Get the exact number
	}
	
	@Override
	public int sizeWhenSerialized() {
		return conf.inodesBlockSizeBytes;
	}
	
	
	/*@Override
	void fromBuffer(ByteBuffer buffer) throws InsufficientResourcesException{
		lock();
		try {
			int blockSize = Constants.inodesBlockSizeBytes;
			
			//if the buffer does not have enough bytes remaining
			if (buffer.remaining() < blockSize)
				throw new InsufficientResourcesException(String.format("Buffer has less bytes remaining: "
						+ "%d bytes are remaining, %d bytes are required.",buffer.remaining(),blockSize));
			
			//Read the individual inodes from the buffer
			inodes = new Inode[Constants.inodesPerBlock];
			for(int i=0; i<Constants.inodesPerBlock; i++){
				inodes[i] = Inode.fromBuffer(buffer);
			}
		} finally {
			unlock();
		}
	}*/
	
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
