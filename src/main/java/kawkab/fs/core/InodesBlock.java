package kawkab.fs.core;

import java.io.File;
import java.io.IOException;
import java.nio.channels.ByteChannel;

import kawkab.fs.commons.Constants;

public class InodesBlock extends Block {
	private final static String namePrefix = "inb";
	
	private final int blockIndex; //Not saved persistently
	private static boolean bootstraped; //Not saved persistently
	private Inode[] inodes; //Should be initialized in the bootstrap function only.
	
	/*
	 * The access modifier of the constructor is "default" so that it can be packaged as a library. Clients
	 * are not supposed to extend or instantiate this class.
	 */
	/**
	 * @param blockIndex The block number, starting from zero, of this inodesBlock. Although the
	 *         inodesBlocks are sharded across machines, the blockIndex is relative to the whole
	 *         system, not relative to this machine.
	 */
	InodesBlock(int blockIndex){
		super(new InodesBlockID(blockIndex));
		this.blockIndex = blockIndex;
		//type = BlockType.InodeBlock;
		//Must not initialize inodes array in any constructor.
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
	
	void initInode(long inumber){
		int inodeNumber = inodeIdxFromInumber(inumber);
		
		lock();
		inodes[inodeNumber] = new Inode(inumber);
		unlock();
	}
	
	@Override
	public void loadFrom(ByteChannel channel) throws IOException {
		lock();
		try {
			inodes = new Inode[Constants.inodesPerBlock];
			for(int i=0; i<Constants.inodesPerBlock; i++){
				inodes[i] = new Inode(0);
				inodes[i].loadFrom(channel);
			}
		} finally {
			unlock();
		}
	}
	
	@Override
	public void storeTo(ByteChannel channel) throws IOException {
		lock();
		try {
			for(Inode inode : inodes) {
				inode.storeTo(channel);
			}
		} finally {
			unlock();
		}
	}
	
	@Override
	public int channelOffset() {
		return 0;
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
	
	@Override
	String name() {
		return name(blockIndex);
	}
	
	public static String name(int blockIndex){
		return namePrefix+blockIndex;
	}
	
	@Override
	String localPath(){
		return Constants.inodeBlocksPath + File.separator + 
				(blockIndex/Constants.inodeBlocksPerDirectory) + File.separator + 
				name(blockIndex);
	}
	
	@Override
	int blockSize(){
		return Constants.inodesBlockSizeBytes;
	}
	
	/**
	 * Creates inodeBlocks on local disk belonging to this machine.
	 * @throws IOException
	 */
	static void bootstrap() throws IOException{
		if (bootstraped)
			return;
		
		File folder = new File(Constants.inodeBlocksPath);
		if (!folder.exists()){
			System.out.println("  Creating folder: " + folder.getAbsolutePath());
			folder.mkdirs();
		}
		
		//LocalStore storage = LocalStore.instance();
		Cache cache = Cache.instance();
		int offset = Constants.inodesBlocksRangeStart;
		for(int i=0; i<Constants.inodeBlocksPerMachine; i++){
			InodesBlock block = new InodesBlock(offset+i);
			block.inodes = new Inode[Constants.inodesPerBlock];
			for (int j=0; j<Constants.inodesPerBlock; j++) {
				long inumber = i*Constants.inodesPerBlock + j;
				block.inodes[j] = new Inode(inumber);
			}
			
			File file = new File(block.localPath());
			if (!file.exists()) {
				//storage.writeBlock(block);
				cache.createBlock(block);
			}
		}
		
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
