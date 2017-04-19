package kawkab.fs.core;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.BitSet;

import kawkab.fs.commons.Constants;
import kawkab.fs.core.exceptions.InodeNumberOutOfRangeException;
import kawkab.fs.core.exceptions.InsufficientResourcesException;

public class Ibmap extends Block{
	private static final int bitsPerByte = 8;
	
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
	Ibmap(int blockIndex){
		super(new BlockID(Constants.ibmapUuidHigh, blockIndex, name(blockIndex), BlockType.IbmapBlock));
		this.blockIndex = blockIndex;
		
	}
	
	/**
	 * This function consumes the next unused inumber from the current Ibmap block.
	 * @return Returns next unused inumber, or -1 if the block is full.
	 */
	long nextInode(){
		int bitIdx = -1;
		
		lock.lock();
		try {
			try{
				bitIdx = bitset.nextClearBit(0);
			}catch(IndexOutOfBoundsException e){
				return -1;
			}
			
			bitset.set(bitIdx);
			dirty = true;
		} finally {
			lock.unlock();
		}
		
		long inumber = bitIndexToInumber(blockIndex, bitIdx); //Convert the bit index to inumber
		
		return inumber;
	}
	
	private long bitIndexToInumber(int blockIndex, int bitIndex){
		return (8L*blockIndex*Constants.ibmapBlockSizeBytes) + bitIndex;
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
		long maxInumber = bitIndexToInumber(blockIndex, Constants.ibmapBlockSizeBytes*8 - 1);
		if (inumber < 0 || inumber > maxInumber)
			throw new InodeNumberOutOfRangeException();
		
		//bitIdx associated with the given inumber
		int bitIdx = inumberToBitIndex(inumber); //inumber % ibmapBlockSize in bits
		
		lock.lock();
		try{
			bitset.clear(bitIdx);     //mark the bit as unused
			dirty = true;
		}finally{
			lock.unlock();
		}
		
	}
	
	@Override
	void fromBuffer(ByteBuffer buffer) throws InsufficientResourcesException{
		lock.lock();
		try {
			int blockSize = Constants.ibmapBlockSizeBytes;
			if (buffer.remaining() < blockSize)
				throw new InsufficientResourcesException(String.format("Buffer has less bytes remaining: "
						+ "%d bytes are remaining, %d bytes are required.",buffer.remaining(),blockSize));
			
			byte[] bytes = new byte[Constants.ibmapBlockSizeBytes];
			buffer.get(bytes);
			bitset = BitSet.valueOf(bytes); //TODO: Convert it to long array.
		} finally {
			lock.unlock();
		}
	}
	
	@Override
	void toBuffer(ByteBuffer buffer) throws InsufficientResourcesException{
		lock.lock();
		try {
			int blockSize = Constants.ibmapBlockSizeBytes;
			if (buffer.capacity() < blockSize)
				throw new InsufficientResourcesException(String.format("Buffer capacity is less than "
							+ "required: Capacity = %d bytes, required = %d bytes.",buffer.capacity(),blockSize));
			
			byte[] bytes = bitset.toByteArray();
			buffer.put(bytes);
		} finally {
			lock.unlock();
		}
	}
	
	/**
	 * Bootstraps the ibmap blocks for this machine. This should be called only once when the filesystem
	 * is formatted for the first use. It creates ibmap block files in the local storage at the
	 * path localPath().
	 * 
	 * @throws IOException
	 */
	static void bootstrap() throws IOException{
		if (bootstraped)
			return;
		
		File folder = new File(Constants.ibmapsPath);
		if (!folder.exists()){
			System.out.println("  Creating folder: " + folder.getAbsolutePath());
			folder.mkdirs();
		}
		
		LocalStore storage = LocalStore.instance();
		int offset = Constants.ibmapBlocksRangeStart;
		for(int i=0; i<Constants.ibmapBlocksPerMachine; i++){
			Ibmap ibmap = new Ibmap(offset+i);
			ibmap.bitset = new BitSet(Constants.ibmapBlockSizeBytes*8);
			
			File file = new File(ibmap.localPath());
			if (!file.exists()){
				storage.writeBlock(ibmap);
			}
		}
		
		bootstraped = true;
	}
	
	boolean bootstraped(){
		return bootstraped;
	}
	
	@Override
	String name() {
		return name(blockIndex);
	}
	
	static String name(int blockIndex){
		return "ibmap"+blockIndex;
	}
	
	@Override
	String localPath(){
		return Constants.ibmapsPath +File.separator+ name(blockIndex);
	}
	
	@Override
	int blockSize(){
		return Constants.ibmapBlockSizeBytes;
	}
	
	static void shutdown(){
		System.out.println("Closing Ibmaps");
		//TODO: stop new requests
	}
}
