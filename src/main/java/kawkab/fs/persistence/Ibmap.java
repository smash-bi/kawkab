package kawkab.fs.persistence;

import java.nio.ByteBuffer;
import java.util.BitSet;

import kawkab.fs.commons.Constants;
import kawkab.fs.core.exceptions.InodeNumberOutOfRangeException;

public class Ibmap {
	//private byte[] bytes;
	private int blockIndex;
	private BitSet bitset;
	private boolean dirty;
	
	public Ibmap(int blockIndex){
		this.blockIndex = blockIndex;
		//bytes = new byte[Constants.ibmapBlockSizeBytes];
		bitset = new BitSet(Constants.ibmapBlockSizeBytes*8);
	}
	
	/**
	 * @return Returns next unused inumber, or -1 if the block is full.
	 */
	public long nextInode(){
		int bitIdx = -1;
		try{
			bitIdx = bitset.nextClearBit(0);
		}catch(IndexOutOfBoundsException e){
			return -1;
		}
		
		bitset.set(bitIdx);
		return (8L*blockIndex*Constants.ibmapBlockSizeBytes) + bitIdx;
	}
	
	public void unlinkInode(int inumber) throws InodeNumberOutOfRangeException{
		if (inumber < 0 || inumber > (blockIndex*Constants.ibmapBlockSizeBytes + Constants.ibmapBlockSizeBytes))
			throw new InodeNumberOutOfRangeException();
		
		int bitIdx = inumber % (Constants.ibmapBlockSizeBytes*8);
		bitset.clear(bitIdx);
	}
	
	public void fromBuffer(ByteBuffer buffer){
		byte[] bytes = new byte[Constants.ibmapBlockSizeBytes];
		buffer.get(bytes);
		bitset = BitSet.valueOf(bytes); //TODO: Convert it to long array.
	}
	
	public void toBuffer(ByteBuffer buffer){
		byte[] bytes = bitset.toByteArray(); //TODO: Convert it to long array.
		buffer.put(bytes);
	}
	
	/*public int consumeInode(){
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
	
	public boolean unlinkInode(int inodeNumber) throws InodeNumberOutOfRangeException{
		if (inodeNumber < 0 || inodeNumber > (blockIndex*Constants.ibmapBlockSizeBytes + Constants.ibmapBlockSizeBytes))
			throw new InodeNumberOutOfRangeException();
		
		int bitNumber = inodeNumber % (Constants.ibmapBlockSizeBytes*8);
		int byteIdx = bitNumber / 8;
		int bitIdx = bitNumber % 8;
		
		boolean alreadyClear = ~(bytes[byteIdx] & 0xFF) == (1 << bitIdx);
		bytes[byteIdx] = (byte)(~(1 << bitIdx) & bytes[byteIdx]);
		
		return alreadyClear;
	}*/
	
	public boolean dirty(){
		return dirty;
	}
	
	public void clear() {
		dirty = false;
	}
}
