package kawkab.fs.core;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.BitSet;

import javax.naming.OperationNotSupportedException;

import kawkab.fs.commons.Commons;
import kawkab.fs.commons.Constants;
import kawkab.fs.core.exceptions.FileNotExistException;
import kawkab.fs.core.exceptions.InodeNumberOutOfRangeException;
import kawkab.fs.core.exceptions.InsufficientResourcesException;
import kawkab.fs.core.exceptions.KawkabException;

public final class Ibmap extends Block{
	private static final int bitsPerByte = Byte.SIZE;
	
	private final int blockIndex; //Not saved persistently
	
	/**
	 * The size of BitSet is equal to the size of Consants.IbmapBlockSizeBytes.
	 * It is initialized in the bootstrap function.
	 */
	private BitSet bitset;
	
	private static boolean bootstraped;
	
	/**
	 * @param blockIndex Index of the Ibmap block for the current machine.
	 */
	Ibmap(IbmapBlockID id) {
		super(id);
		this.blockIndex = id.blockIndex();
		byte[] bytes = new byte[Constants.ibmapBlockSizeBytes];
		bitset = BitSet.valueOf(bytes);
		markDirty();
	}
	
	/**
	 * This function consumes the next unused inumber from the current Ibmap block.
	 * @return Returns next unused inumber, or -1 if the block is full.
	 */
	long nextInode(){
		int bitIdx = -1;
		
		lock();
		try {
			bitIdx = bitset.nextClearBit(0);
			bitset.set(bitIdx);
			markDirty();
		} catch(IndexOutOfBoundsException e){
			return -1;
		} finally {
			unlock();
		}
		
		long inumber = bitIdxToInumber(blockIndex, bitIdx); //Convert the bit index to inumber
		
		return inumber;
	}
	
	private long bitIdxToInumber(int ibmapIdx, int bitIndex){
		long inumber = (8L*ibmapIdx*Constants.ibmapBlockSizeBytes) + bitIndex;
		System.out.println("[Ibmap] blockIdx: " + ibmapIdx + ", bitIdx: " + bitIndex + ", inumber: " + inumber);
		return inumber;
	}
	
	private int inumberToBitIndex(long inumber){
		return (int)(inumber % (Constants.ibmapBlockSizeBytes * bitsPerByte));
	}
	
	/**
	 * Marks the inode as unused, which is equivalent to deleting the file.
	 * @param inumber Inode number associated with the file
	 * @throws InodeNumberOutOfRangeException if the inumber is out of range of this ibmap block.
	 */
	void unlinkInode(long inumber) throws InodeNumberOutOfRangeException{
		//The inumber associated with the last bit of this ibmap block.
		long maxInumber = bitIdxToInumber(blockIndex, Constants.ibmapBlockSizeBytes*8 - 1);
		if (inumber < 0 || inumber > maxInumber)
			throw new InodeNumberOutOfRangeException();
		
		//bitIdx associated with the given inumber
		int bitIdx = inumberToBitIndex(inumber); //inumber % ibmapBlockSize in bits
		
		lock();
		try{
			bitset.clear(bitIdx);     //mark the bit as unused
			markDirty();
		}finally{
			unlock();
		}
	}
	
	@Override
	public boolean shouldStoreGlobally() {
		return true;
	}
	
	@Override
	public void loadFrom(ByteBuffer buffer) throws IOException {
		if (buffer.remaining() < Constants.ibmapBlockSizeBytes) {
			throw new InsufficientResourcesException(String.format("Not enough bytes left in the buffer: "
					+ "Have %d, needed %d.",buffer.remaining(), Constants.ibmapBlockSizeBytes));
		}
		
		byte[] bytes = new byte[Constants.ibmapBlockSizeBytes];
		buffer.get(bytes);
		
		bitset = BitSet.valueOf(bytes);
	}
	
	@Override
	public void loadFrom(ReadableByteChannel channel) throws IOException {
		byte[] bytes = new byte[Constants.ibmapBlockSizeBytes];
		ByteBuffer buffer = ByteBuffer.wrap(bytes);
		int bytesRead = Commons.readFrom(channel, buffer);
		if (bytesRead < bytes.length)
			throw new InsufficientResourcesException(String.format("Full block is not loaded. Loaded "
					+ "%d bytes out of %d.",bytesRead,bytes.length));
		
		bitset = BitSet.valueOf(bytes);
	}
	
	/*@Override
	public int fromInputStream(InputStream in) throws IOException {
		byte[] bytes = new byte[Constants.ibmapBlockSizeBytes];
		int read = 0;
		int remaining = bytes.length;
		int ret = 0;
		while(remaining > 0 && ret >= 0) {
			ret = in.read(bytes, read, remaining);
			remaining -= ret;
			read += ret;
		}
		
		if (read != bytes.length)
			throw new IOException("Unable to load Ibmap completely from the inputstream: " + name());
		
		bitset = BitSet.valueOf(bytes);
		
		return read;
	}*/
	
	@Override
	public int storeTo(WritableByteChannel channel) throws IOException {
		return storeFullTo(channel);
	}
	
	@Override
	public int storeFullTo(WritableByteChannel channel) throws IOException {
		ByteBuffer buffer = ByteBuffer.allocate(Constants.ibmapBlockSizeBytes);
		buffer.put(bitset.toByteArray());
		buffer.flip();
		buffer.limit(buffer.capacity());
		
		int bytesWritten = Commons.writeTo(channel, buffer);
		if (bytesWritten < buffer.capacity()) {
			throw new InsufficientResourcesException(String.format("Full block is not strored. Stored "
					+ "%d bytes out of %d.",bytesWritten, buffer.capacity()));
		}
		
		return bytesWritten;
	}

	@Override
	public ByteArrayInputStream getInputStream() {
		return new ByteArrayInputStream(bitset.toByteArray());
	}
	
	@Override
	protected void getFromPrimary()  throws FileNotExistException, KawkabException, IOException {
		throw new KawkabException(new OperationNotSupportedException());
	}

	@Override
	public int appendOffsetInBlock() {
		return 0;
	}
	
	@Override
	public int memorySizeBytes() {
		return Constants.ibmapBlockSizeBytes + 16; //FIXME: Get the exact number
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
	 * @throws KawkabException 
	 */
	static void bootstrap() throws IOException, InterruptedException, KawkabException{
		if (bootstraped)
			return;
		
		File folder = new File(Constants.ibmapsPath);
		if (!folder.exists()) {
			System.out.println("  Creating folder: " + folder.getAbsolutePath());
			folder.mkdirs();
		}
		
		//LocalStore storage = LocalStore.instance();
		Cache cache = Cache.instance();
		int rangeStart = Constants.ibmapBlocksRangeStart;
		int rangeEnd = rangeStart + Constants.ibmapsPerMachine;
		for(int i=rangeStart; i<rangeEnd; i++){
			IbmapBlockID id = new IbmapBlockID(i);
			try {
				cache.acquireBlock(id, true); // This will create a new block in the local store. Moreover, the block is marked dirty in the constructor.
			} finally {
				cache.releaseBlock(id);
			}
			
			/*Ibmap ibmap = new Ibmap(i);
			ibmap.bitset = new BitSet(Constants.ibmapBlockSizeBytes*8);
			
			File file = new File(ibmap.id().localPath());
			if (!file.exists()) {
				cache.createBlock(ibmap);
			}*/
		}
		
		bootstraped = true;
	}
	
	boolean bootstraped(){
		return bootstraped;
	}
	
	/*@Override
	String name() {
		return id.name();
	}
	
	static String name(int blockIndex) {
		return IbmapBlockID.name(blockIndex);
	}
	
	@Override
	String localPath(){
		return id.localPath();
	}*/
	
	static void shutdown(){
		System.out.println("Closing Ibmaps");
		//TODO: stop new requests
	}
}
