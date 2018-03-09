package kawkab.fs.core;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import kawkab.fs.commons.Constants;
import kawkab.fs.core.exceptions.FileNotExistException;
import kawkab.fs.core.exceptions.KawkabException;

public final class InodesBlock extends Block {
	private static boolean bootstraped; //Not saved persistently
	private Inode[] inodes; //Should be initialized in the bootstrap function only.
	//private int version; //Inodes-block's current version number.
	
	/*
	 * The access modifier of the constructor is "default" so that it can be packaged as a library. Clients
	 * are not supposed to extend or instantiate this class.
	 */
	/**
	 * @param blockIndex The block number, starting from zero, of this inodesBlock. Although the
	 *         inodesBlocks are sharded across machines, the blockIndex is relative to the whole
	 *         system, not relative to this machine.
	 * @param version This the version number of this inodesBlock. The current version number should be retrieved
	 *        from the inodesBlock version table (IBV table).
	 */
	InodesBlock(InodesBlockID id){
		super(id);
		
		inodes = new Inode[Constants.inodesPerBlock];
		int blockIndex = id.blockIndex();
		for (int j=0; j<Constants.inodesPerBlock; j++) {
			long inumber = blockIndex*Constants.inodesPerBlock + j;
			initInode(inumber);
		}
	}
	
	protected void initInode(long inumber) {
		int inumberIdx = inodeIdxFromInumber(inumber);
		inodes[inumberIdx] = new Inode(inumber);
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
		return true;
	}
	
	@Override
	public boolean evictLocallyOnMemoryEviction() {
		return false;
	}
	
	@Override
	public void loadFrom(ByteBuffer buffer) throws IOException {
		lock();
		try {
			//inodes = new Inode[Constants.inodesPerBlock];
			for(int i=0; i<Constants.inodesPerBlock; i++){
				//inodes[i] = new Inode(0);
				inodes[i].loadFrom(buffer);
			}
		} finally {
			unlock();
		}
	}
	
	@Override
	public void loadFrom(ReadableByteChannel channel) throws IOException {
		lock();
		try {
			//inodes = new Inode[Constants.inodesPerBlock];
			for(int i=0; i<Constants.inodesPerBlock; i++){
				//inodes[i] = new Inode(0);
				inodes[i].loadFrom(channel);
			}
		} finally {
			unlock();
		}
	}
	
	@Override
	public int storeTo(WritableByteChannel channel) throws IOException {
		return storeFullTo(channel);
	}
	
	@Override
	public int storeFullTo(WritableByteChannel channel) throws IOException {
		int bytesWritten = 0;
		lock();
		try {
			for(Inode inode : inodes) {
				bytesWritten += inode.storeTo(channel);
			}
		} finally {
			unlock();
		}
		
		return bytesWritten;
	}
	
	@Override 
	public ByteArrayInputStream getInputStream() {
		//TODO: This function takes extra memory to serialize inodes in an input stream. We need an alternate
		//method for this purpose.
		ByteBuffer buffer = ByteBuffer.allocate(inodes.length * Constants.inodeSizeBytes);
		lock();
		try {
			for(Inode inode : inodes) {
				inode.storeTo(buffer);
			}
		} finally {
			unlock();
		}
		buffer.flip();
		return new ByteArrayInputStream(buffer.array());
	}
	
	@Override
	protected void loadBlockFromPrimary()  throws FileNotExistException, KawkabException, IOException {
		primaryNodeService.getInodesBlock((InodesBlockID)id(), this);
	}

	@Override
	public int appendOffsetInBlock() {
		return 0;
	}
	
	@Override
	public int memorySizeBytes() {
		return Constants.inodesBlockSizeBytes + 8; //FIXME: Get the exact number
	}
	
	@Override
	public int sizeWhenSerialized() {
		return Constants.inodesBlockSizeBytes;
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
		
		File folder = new File(Constants.inodeBlocksPath);
		if (!folder.exists()){
			System.out.println("  Creating folder: " + folder.getAbsolutePath());
			folder.mkdirs();
		}
		
		int count=0;
		//LocalStore storage = LocalStore.instance();
		Cache cache = Cache.instance();
		int rangeStart = Constants.inodeBlocksRangeStart;
		int rangeEnd = rangeStart + Constants.inodeBlocksPerMachine;
		for(int i=rangeStart; i<rangeEnd; i++){
			InodesBlockID id = new InodesBlockID(i);
			File file = new File(id.localPath());
			if (!file.exists()) {
				try {
					Block block = cache.acquireBlock(id, true); // Create a new block in the local store
					block.markDirty(); // Mark the new block as dirty to write the initialized inodes
				} finally {
					cache.releaseBlock(id);
				}
			}
		}
		
		System.out.println("Created inodeBlock files: " + count);
		bootstraped = true;
	}
	
	static void shutdown(){
		System.out.println("Closing InodesBlock");
	}
	
	static int blockIndexFromInumber(long inumber) {
		return (int)(inumber / Constants.inodesPerBlock);
	}
	
	private int inodeIdxFromInumber(long inumber){
		return (int)(inumber % Constants.inodesPerBlock);
	}
}
