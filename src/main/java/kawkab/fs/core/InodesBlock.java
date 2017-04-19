package kawkab.fs.core;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import kawkab.fs.commons.Constants;
import kawkab.fs.core.exceptions.InsufficientResourcesException;

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
		super(new BlockID(Constants.inodesBlocksUuidHigh, blockIndex, InodesBlock.name(blockIndex), BlockType.InodeBlock));
		this.blockIndex = blockIndex;
		//Must not initialize inodes array in any constructor.
	}
	
	/**
	 * @param iNumber iNumber is the inode number belonging to the file. This 
	 *         should be an absolute number, not relative to this inodesBock. However, the inode
	 *         associated with the iNumber must belong to this block. This function converts the
	 *         inumber to the inode in the block in the following way: 
	 *         int inodeIndexInBlock = (int)(iNumber % Constants.inodesPerBlock);
	 *         Therefore, the caller of this function must ensure that the correct inodesBlock is
	 *         selected.
	 *         
	 *         The caller of this function must flush this block if append succeeds. This function
	 *         causes this inodesBlock, the dataBlock(s), and potentially some indexBlock(s) (which are
	 *         wrapper around dataBlocks) to be updated. The dataBlocks and indexBlock(s) get flushed
	 *         before this inodesBlock.
	 *         
	 *         It is possible that several independent file writers call this function concurrently,
	 *         and update different inodes. We need a mechanism to reduce the number of flushes.
	 *         Moreover, we need a mechanism that allows many writers to update different files
	 *         while sharing the same inodes block. Different files have different inodes but they
	 *         may share a common inodesBlock.
	 *         
	 * @param data Data to be appended.
	 * @param offset Offset in the data array
	 * @param length number of bytes to be appended from the data array.
	 * @return
	 * @throws MaxFileSizeExceededException if the file has reached its capacity
	 * @throws InvalidFileOffsetException if the offset in the file is wrong.
	 */
	/*public int append(long iNumber, byte[] data, int offset, int length) 
							throws MaxFileSizeExceededException, InvalidFileOffsetException {
		int len;
		try {
			writeLock.lock(); //FIXME: This prevents updating different files that share this inodesBlock.
			
			//Convert the iNumber to the index of the inode in this block.
			int inodeIndexInBlock = inodeIdxFromInumber(iNumber);
			
			//This append function modifies the inode. Therefore, we should mark this inodes block
			//as dirty, and we must flush this inodes block.
			len = inodes[inodeIndexInBlock].append(data, offset, length);
			dirty = inodes[inodeIndexInBlock].dirty();
		} finally {
			writeLock.unlock();
		}
		
		return len;
	}*/
	
//	/**
//	 * @param iNumber iNumber is the inode number belonging to the file. This 
//	 *         should be an absolute number, not relative to this inodesBock. However, the inode
//	 *         associated with the iNumber must belong to this block. This function converts the
//	 *         inumber to the inode in the block in the following way: 
//	 *         int inodeIndexInBlock = (int)(iNumber % Constants.inodesPerBlock);
//	 *         Therefore, the caller of this function must ensure that the correct inodesBlock is
//	 *         selected.
//	 *         
//	 * @param buffer 
//	 * @param length Number of bytes to read.
//	 * @param offsetInFile It is the offset relative to the start of the file.
//	 * @return
//	 * @throws InvalidFileOffsetException
//	 * @throws InvalidArgumentsException
//	 */
//	public int read(long iNumber, byte[] buffer, int length, long offsetInFile) throws 
//			InvalidFileOffsetException, InvalidArgumentsException {
//		int inodeIndexInBlock = inodeIdxFromInumber(iNumber);
//		int len = 0;
//		len = inodes[inodeIndexInBlock].read(buffer, length, offsetInFile);
//		
//		return len;
//	}
	
	
	/**
	 * @param inumber 
	 * @return Returns the Inode corresponding to the inumber
	 */
	public Inode getInode(long inumber){
		return inodes[inodeIdxFromInumber(inumber)];
	}
	
	/*
	 * The caller should lock this inodesBlock before calling this funciton. 
	 */
	public void markDirty(){
		dirty = true;
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
		
		lock.lock();
		try {
			inodes[inodeNumber] = new Inode();
		}finally {
			lock.unlock();
		}
	}
	
	@Override
	void fromBuffer(ByteBuffer buffer) throws InsufficientResourcesException{
		lock.lock();
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
		}finally{
			lock.unlock();
		}
	}
	
	/*
	 * (non-Javadoc)
	 * @see kawkab.fs.core.Block#toBuffer(java.nio.ByteBuffer)
	 */
	@Override
	void toBuffer(ByteBuffer buffer) throws InsufficientResourcesException{
		//FIXME: Do we need to acquire the lock here??? We should not! Otherwise, we will have a high performance hit.
		
		int blockSize = Constants.inodesBlockSizeBytes;
		if (buffer.capacity() < blockSize)
			throw new InsufficientResourcesException(String.format("Buffer capacity is less than "
						+ "required: Capacity = %d bytes, required = %d bytes.",buffer.capacity(),blockSize));

		for(Inode inode : inodes) {
			inode.toBuffer(buffer);
		}
	}
	
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
		
		LocalStore storage = LocalStore.instance();
		int offset = Constants.inodesBlocksRangeStart;
		for(int i=0; i<Constants.inodeBlocksPerMachine; i++){
			InodesBlock block = new InodesBlock(offset+i);
			block.inodes = new Inode[Constants.inodesPerBlock];
			for (int j=0; j<Constants.inodesPerBlock; j++) {
				block.inodes[j] = new Inode();
			}
			
			File file = new File(block.localPath());
			if (!file.exists()){
				storage.writeBlock(block);
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
