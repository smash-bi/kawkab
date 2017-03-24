package kawkab.fs.core;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.BitSet;

import kawkab.fs.commons.Constants;
import kawkab.fs.core.exceptions.InodeNumberOutOfRangeException;
import kawkab.fs.core.exceptions.InsufficientResourcesException;

public class Ibmap extends Block{
	//private byte[] bytes;
	private final int blockIndex; //Not saved persistently
	private BitSet bitset;
	private boolean dirty;
	private static boolean bootstraped;
	
	Ibmap(int blockIndex){
		this.blockIndex = blockIndex;
	}
	
	/**
	 * @return Returns next unused inumber, or -1 if the block is full.
	 */
	long nextInode(){
		int bitIdx = -1;
		try{
			bitIdx = bitset.nextClearBit(0);
		}catch(IndexOutOfBoundsException e){
			return -1;
		}
		
		bitset.set(bitIdx);
		
		dirty = true;
		return (8L*blockIndex*Constants.ibmapBlockSizeBytes) + bitIdx;
	}
	
	void unlinkInode(int inumber) throws InodeNumberOutOfRangeException{
		if (inumber < 0 || inumber > (blockIndex*Constants.ibmapBlockSizeBytes + Constants.ibmapBlockSizeBytes))
			throw new InodeNumberOutOfRangeException();
		
		int bitIdx = inumber % (Constants.ibmapBlockSizeBytes*8);
		bitset.clear(bitIdx);
		
		dirty = true;
	}
	
	@Override
	void fromBuffer(ByteBuffer buffer) throws InsufficientResourcesException{
		int blockSize = Constants.ibmapBlockSizeBytes;
		if (buffer.remaining() < blockSize)
			throw new InsufficientResourcesException(String.format("Buffer has less bytes remaining: "
					+ "%d bytes are remaining, %d bytes are required.",buffer.remaining(),blockSize));
		
		byte[] bytes = new byte[Constants.ibmapBlockSizeBytes];
		buffer.get(bytes);
		bitset = BitSet.valueOf(bytes); //TODO: Convert it to long array.
	}
	
	@Override
	void toBuffer(ByteBuffer buffer) throws InsufficientResourcesException{
		int blockSize = Constants.ibmapBlockSizeBytes;
		if (buffer.capacity() < blockSize)
			throw new InsufficientResourcesException(String.format("Buffer capacity is less than "
						+ "required: Capacity = %d bytes, required = %d bytes.",buffer.capacity(),blockSize));
		
		byte[] bytes = bitset.toByteArray(); //TODO: Convert it to long array.
		buffer.put(bytes);
	}
	
	/*int consumeInode(){
		int byteIdx = 0;
		for(byteIdx=0; byteIdx<bytes.length; byteIdx++){
			if ((bytes[byteIdx] & 0xFF) != 0xFF)
				break;
		}
		
		if (byteIdx == bytes.length)
			return -1;
		
		int bitIdx = 0;
		int b = bytes[byteIdx];
		for (bitIdx=0; bitIdx<8; bitIdx++){
			if ((b & 0x01) == 0)
				break;
			
			b = b << 1;
		}
		
		bytes[byteIdx] = (byte)((bytes[byteIdx] & 0xFF) | (1 << bitIdx));
		dirty = true;
		
		return (blockIndex*Constants.ibmapBlockSizeBytes) + (byteIdx*8) + bitIdx;
	}
	
	boolean unlinkInode(int inodeNumber) throws InodeNumberOutOfRangeException{
		if (inodeNumber < 0 || inodeNumber > (blockIndex*Constants.ibmapBlockSizeBytes + Constants.ibmapBlockSizeBytes))
			throw new InodeNumberOutOfRangeException();
		
		int bitNumber = inodeNumber % (Constants.ibmapBlockSizeBytes*8);
		int byteIdx = bitNumber / 8;
		int bitIdx = bitNumber % 8;
		
		boolean alreadyClear = ~(bytes[byteIdx] & 0xFF) == (1 << bitIdx);
		bytes[byteIdx] = (byte)(~(1 << bitIdx) & bytes[byteIdx]);
		
		return alreadyClear;
	}*/
	
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
	boolean dirty(){
		return dirty;
	}
	
	@Override
	void clearDirty() {
		dirty = false;
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
		return Constants.ibmapsPath + "/" + name(blockIndex);
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
