package kawkab.fs.core;

import kawkab.fs.commons.Commons;
import kawkab.fs.commons.Configuration;
import kawkab.fs.core.exceptions.InodeNumberOutOfRangeException;
import kawkab.fs.core.exceptions.InsufficientResourcesException;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.util.BitSet;

public final class Ibmap extends Block{
	private static final int bitsPerByte = Byte.SIZE;
	private static final int ibmapBlockSizeBytes = Configuration.instance().ibmapBlockSizeBytes;
			
	private final int blockIndex; //Not saved persistently
	
	/**
	 * The size of BitSet is equal to the size of Consants.IbmapBlockSizeBytes.
	 * It is initialized in the bootstrap function.
	 */
	private BitSet bitset;
	
	private static boolean bootstraped;
	
	Ibmap(IbmapBlockID id) {
		super(id);
		this.blockIndex = id.blockIndex();
		byte[] bytes = new byte[ibmapBlockSizeBytes];
		bitset = BitSet.valueOf(bytes);
	}
	
	/**
	 * This function consumes the next unused inumber from the current Ibmap block.
	 * @return Returns next unused inumber, or -1 if the block is full.
	 */
	synchronized long useNextInumber(){
		int bitIdx;
		
		try {
			bitIdx = bitset.nextClearBit(0);
			bitset.set(bitIdx);
			markLocalDirty();
		} catch(IndexOutOfBoundsException e){
			return -1;
		}
		
		long inumber = bitIdxToInumber(blockIndex, bitIdx); //Convert the bit index to inumber
		
		markLocalDirty();
		
		return inumber;
	}
	
	private long bitIdxToInumber(int ibmapIdx, int bitIndex){
		return (8L*ibmapIdx*ibmapBlockSizeBytes) + bitIndex;
	}
	
	private int inumberToBitIndex(long inumber){
		return (int)(inumber % (ibmapBlockSizeBytes * bitsPerByte));
	}
	
	/**
	 * Marks the inode as unused, which is equivalent to deleting the file.
	 * @param inumber Inode number associated with the file
	 * @throws InodeNumberOutOfRangeException if the inumber is out of range of this ibmap block.
	 */
	synchronized void unlinkInode(long inumber) throws InodeNumberOutOfRangeException{
		//The inumber associated with the last bit of this ibmap block.
		long maxInumber = bitIdxToInumber(blockIndex, ibmapBlockSizeBytes*8 - 1);
		if (inumber < 0 || inumber > maxInumber)
			throw new InodeNumberOutOfRangeException();
		
		//bitIdx associated with the given inumber
		int bitIdx = inumberToBitIndex(inumber); //inumber % ibmapBlockSize in bits
		
		bitset.clear(bitIdx);     //mark the bit as unused
		markLocalDirty();
	}
	
	@Override
	public boolean shouldStoreGlobally() {
		return true;
		//return false; //FIXME: Disabled ibmaps transfer to the GlobalStore
	}
	
	@Override
	public boolean evictLocallyOnMemoryEviction() {
		return false;
	}
	
	@Override
	public synchronized int loadFrom(ByteBuffer buffer) throws IOException {
		if (buffer.remaining() < ibmapBlockSizeBytes) {
			throw new InsufficientResourcesException(String.format("Not enough bytes left in the buffer: "
					+ "Have %d, needed %d.",buffer.remaining(), ibmapBlockSizeBytes));
		}
		
		byte[] bytes = new byte[ibmapBlockSizeBytes];
		buffer.get(bytes);
		
		bitset = BitSet.valueOf(bytes);

		return bytes.length;
	}
	
	@Override
	public synchronized int loadFromFile() throws IOException {
		byte[] bytes = Files.readAllBytes(new File(id.localPath()).toPath());
		bitset = BitSet.valueOf(bytes);
		return bytes.length;
	}
	
	@Override
	public synchronized int loadFrom(ReadableByteChannel channel) throws IOException {
		byte[] bytes = new byte[ibmapBlockSizeBytes];
		ByteBuffer buffer = ByteBuffer.wrap(bytes);
		int bytesRead = Commons.readFrom(channel, buffer);
		
		bitset = BitSet.valueOf(bytes);
		
		System.out.println("[I] Number of inodes already in use: "+bitset.nextClearBit(0));

		return bytesRead;
	}
	
	@Override
	protected void loadBlockOnNonPrimary(boolean loadFromPrimary) {
		assert false; //Loading ibmaps on a non-primary node is not allowed as each node has the ownership of a set of ibmaps
	}
	
	@Override
	public synchronized int storeToFile() throws IOException {
		byte[] bytes = bitset.toByteArray();
		Files.write(new File(id.localPath()).toPath(), bytes);
		return bytes.length;
	}
	
	@Override
	public synchronized int storeTo(FileChannel channel) throws IOException {
		ByteBuffer buffer = ByteBuffer.allocate(ibmapBlockSizeBytes);
		buffer.put(bitset.toByteArray());
		buffer.rewind();
		
		channel.position(0);
		
		int bytesWritten = Commons.writeTo(channel, buffer);
		if (bytesWritten < buffer.capacity()) {
			throw new InsufficientResourcesException(String.format("[I] Full block is not strored. Stored "
					+ "%d/%d bytes.",bytesWritten, buffer.capacity()));
		}
		
		//System.out.printf("[I%d] Stored bytes %d\n", blockIndex, bytesWritten);
		
		return bytesWritten;
	}

	@Override
	public int sizeWhenSerialized() {
		//FIXME: This creates a copy in memory. Get size without memory copy.
		//return bitset.toByteArray().length;
		return (bitset.length()+7)/8; //Taken from the documentation of BitSet.toArray() function. 
	}
	
	/**
	 * Bootstraps the ibmap blocks for this machine. This should be called only once when the filesystem
	 * is formatted for the first use. It creates ibmap block files in the local storage at the
	 * path localPath().
	 * 
	 * @throws IOException
	 * @throws InterruptedException 
	 */
	static void bootstrap() throws IOException, InterruptedException {
		if (bootstraped)
			return;
		
		System.out.println("[I] Initializing Ibmaps.");
		
		Configuration conf = Configuration.instance();
		
		File folder = new File(conf.ibmapsPath);
		if (!folder.exists()) {
			System.out.println("  Creating folder: " + folder.getAbsolutePath());
			if (!folder.mkdirs()) {
				throw new IOException("Unable to create parent directories for path: " + folder.getAbsolutePath());
			}
		}
		
		LocalStoreManager storage = LocalStoreManager.instance();
		
		int rangeStart = conf.ibmapBlocksRangeStart;
		int rangeEnd = rangeStart + conf.ibmapsPerMachine;
		
		for(int i=rangeStart; i<rangeEnd; i++){
			IbmapBlockID id = new IbmapBlockID(i);
			File file = new File(id.localPath());
			
			if (!file.exists()) {
				System.out.println("[I] Creating Ibmap: " + file.getCanonicalPath());
				storage.createBlock(id);
				Block block = id.newBlock();
				block.storeToFile();
			}
		}
		
		bootstraped = true;
	}
	
	boolean bootstraped(){
		return bootstraped;
	}
	
	static void shutdown(){
		System.out.println("Closing Ibmaps");
		//TODO: stop new requests
	}
}
