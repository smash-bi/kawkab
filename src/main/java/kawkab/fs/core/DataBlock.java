package kawkab.fs.core;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.UUID;

import kawkab.fs.commons.Commons;
import kawkab.fs.commons.Constants;
import kawkab.fs.core.exceptions.InsufficientResourcesException;
import kawkab.fs.core.exceptions.InvalidFileOffsetException;

public class DataBlock extends Block {
	//private long firstAppendTime;
	//private long lastAppendTime;
	
	private byte[] data;
	private final static int maxBlockSize = Constants.dataBlockSizeBytes;
	public long blockNumber; //FIXME: Used for debugging only.
	
	DataBlock(BlockID uuid){
		super(uuid, BlockType.DataBlock);
		data = new byte[maxBlockSize];
		
		//firstAppendTime = -1;
		//lastAppendTime = -1;
	}
	
	/**
	 * @param data Data to be appended
	 * @param offset  Offset in data
	 * @param length  Number of bytes to append: data[offset] to data[offset+length-1] inclusive.
	 * @param offsetInFile  
	 * @return number of bytes appended
	 */
	synchronized int append(byte[] data, int offset, int length, long offsetInFile){
		assert offset >= 0;
		assert offset < data.length;
		
		int offsetInBlock = (int)(offsetInFile % Constants.dataBlockSizeBytes);
		int capacity = Constants.dataBlockSizeBytes - offsetInBlock;
		
		int bytes = length <= capacity ? length : capacity;
		
		for(int i=0; i<bytes; i++){
			this.data[offsetInBlock] = data[i+offset];
			offsetInBlock++;
		}
		
		/*
		long time = System.currentTimeMillis();
		lastAppendTime = time;
		if (offsetInBlock == length || firstAppendTime == -1)
			firstAppendTime = time;*/
		
		//FIXME: Do we need to grab a lock to mark the data block as dirty???
		lock.lock();
		dirty = true;
		lock.unlock();
		
		return bytes;
	}
	
	synchronized int appendLong(long data, long offsetInFile){
		//TODO: check input bounds
		
		int offsetInBlock = (int)(offsetInFile % Constants.dataBlockSizeBytes);
		int capacity = Constants.dataBlockSizeBytes - offsetInBlock;
		if (capacity < 8)
			return 0;
		
		ByteBuffer buffer = ByteBuffer.wrap(this.data);
		buffer.position(offsetInBlock);
		buffer.putLong(data);
		
		/*long time = System.currentTimeMillis();
		lastAppendTime = time;
		if (offsetInBlock == 0 || firstAppendTime == -1)
			firstAppendTime = time;*/
		
		//FIXME: Do we need to grab a lock to mark the data block as dirty???
		lock.lock();
		dirty = true;
		lock.unlock();
		
		return 8;
	}
	
	synchronized long readLong(long offsetInFile) throws InvalidFileOffsetException{
		//int blockOffset = (int)(fileOffsetBytes % Constants.dataBlockSizeBytes);
		
		int offsetInBlock = (int)(offsetInFile % Constants.dataBlockSizeBytes);
		int blockSize = Constants.dataBlockSizeBytes;
		
		if (offsetInBlock >= blockSize || 
				offsetInBlock+8 > blockSize || offsetInBlock < 0)
			throw new InvalidFileOffsetException(
					String.format("File Offset %d is invalid. Data block size = %d bytes.", 
							offsetInBlock, blockSize));
		
		ByteBuffer buffer = ByteBuffer.wrap(this.data);
		buffer.position(offsetInBlock);
		return buffer.getLong();
	}
	
	synchronized int appendInt(int data, long offsetInFile){
		int offsetInBlock = (int)(offsetInFile % Constants.dataBlockSizeBytes);
		int capacity = Constants.dataBlockSizeBytes - offsetInBlock;
		if (capacity < 4)
			return 0;
		
		ByteBuffer buffer = ByteBuffer.wrap(this.data);
		buffer.position(offsetInBlock);
		buffer.putInt(data);
		
		/*long time = System.currentTimeMillis();
		lastAppendTime = time;
		if (offsetInBlock == 0 || firstAppendTime == -1)
			firstAppendTime = time;*/
		
		//FIXME: Do we need to grab a lock to mark the data block as dirty???
		lock.lock();
		dirty = true;
		lock.unlock();
		
		return 4;
	}
	
	synchronized int readInt(long offsetInFile) throws InvalidFileOffsetException{
		//int blockOffset = (int)(fileOffsetBytes % Constants.dataBlockSizeBytes);
		
		int blockSize = Constants.dataBlockSizeBytes;
		int offsetInBlock = (int)(offsetInFile % Constants.dataBlockSizeBytes);
		if (offsetInBlock >= blockSize || 
				offsetInBlock+4 >= blockSize || offsetInBlock < 0) {
			throw new InvalidFileOffsetException(
					String.format("File Offset %d is invalid. Data block size = %d bytes.", 
							offsetInBlock, blockSize));
		}
		
		ByteBuffer buffer = ByteBuffer.wrap(this.data);
		buffer.position(offsetInBlock);
		return buffer.getInt();
	}
	
	/**
	 * @param buffer output buffer
	 * @param bufferOffset offset in the buffer to where data will be copied
	 * @param length length of data to be read
	 * @param offsetInBlock offset in this block from where to read data, offset starts from zero.
	 * @return number of bytes read
	 * @throws IncorrectOffsetException 
	 */
	synchronized int read(byte[] buffer, int bufferOffset, int length, long offsetInFile) throws InvalidFileOffsetException{
		//int blockOffset = (int)(fileOffsetBytes % Constants.dataBlockSizeBytes);
		
		int blockSize = Constants.dataBlockSizeBytes;
		int offsetInBlock = (int)(offsetInFile % Constants.dataBlockSizeBytes);
		if (offsetInBlock >= blockSize || offsetInBlock < 0) {
			throw new InvalidFileOffsetException(
					String.format("Given file offset %d is outside of the block. Block size = %d bytes.",
					offsetInBlock, blockSize));
		}
		
		int readSize = offsetInBlock+length <= blockSize ? length : blockSize-offsetInBlock;
		for(int i=0; i<readSize; i++){
			buffer[bufferOffset+i] = this.data[offsetInBlock+i];
		}
		
		return readSize;
	}
	
	/*long firstAppendTime(){
		return firstAppendTime;
	}
	
	long lastAppendTime(){
		return lastAppendTime;
	}*/
	
	boolean hasByte(long offsetInFile){
		return false;
	}
	
	BlockID uuid(){
		return id;
	}
	
	byte[] data(){
		return data;
	}
	
	@Override
	void fromBuffer(ByteBuffer buffer) throws InsufficientResourcesException {
		int blockSize = Constants.dataBlockSizeBytes;
		if (buffer.remaining() < blockSize)
			throw new InsufficientResourcesException(String.format("Buffer has less bytes remaining: "
					+ "%d bytes are remaining, %d bytes are required.",buffer.remaining(),blockSize));
		
		data = new byte[Constants.dataBlockSizeBytes];
		buffer.get(data);
	}

	@Override
	void toBuffer(ByteBuffer buffer) throws InsufficientResourcesException {
		int blockSize = Constants.dataBlockSizeBytes;
		if (buffer.capacity() < blockSize)
			throw new InsufficientResourcesException(String.format("Buffer capacity is less than "
						+ "required: Capacity = %d bytes, required = %d bytes.",buffer.capacity(),blockSize));
		
		buffer.put(data);
	}

	@Override
	String name() {
		return Commons.uuidToString(id.uuidHigh, id.uuidLow);
	}
	
	static String name(long uuidHigh, long uuidLow){
		return Commons.uuidToString(uuidHigh, uuidLow);
	}
	
	@Override
	int blockSize(){
		return Constants.dataBlockSizeBytes;
	}

	@Override
	String localPath() {
		String uuid = Commons.uuidToString(id.uuidHigh, id.uuidLow);
		int uuidLen = uuid.length();
		
		assert uuidLen == 24;
		int wordSize = 2;
		int levels = 3;
		
		StringBuilder path = new StringBuilder(Constants.blocksPath.length()+uuidLen+levels);
		path.append(Constants.blocksPath);
		for (int i=0; i<levels; i++){
			path.append(File.separator).append(uuid.substring(i*wordSize, i*wordSize+wordSize));
		}
		path.append(File.separator).append(uuid.substring(levels*wordSize));
		
		return path.toString();
	}
	
	public static BlockID randomID(){
		UUID uuid = UUID.randomUUID();
		long uuidHigh = uuid.getMostSignificantBits();
		long uuidLow = uuid.getLeastSignificantBits();
		return new BlockID(uuidHigh, uuidLow, DataBlock.name(uuidHigh, uuidLow), BlockType.DataBlock);
	}
	
	@Override
	public String toString(){
		return blockNumber+"-"+name();
	}
}