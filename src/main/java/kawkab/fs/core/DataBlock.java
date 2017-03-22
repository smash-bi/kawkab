package kawkab.fs.core;

import java.nio.ByteBuffer;

import kawkab.fs.commons.Constants;
import kawkab.fs.core.exceptions.InvalidFileOffsetException;

public class DataBlock {
	private final long uuidLow;
	private final long uuidHigh;
	//private final long blockNumber; //starts from 0.
	//private final long offsetInFile; //offset of the first data byte relative to the start of the file
	private final long createTime;
	private long firstAppendTime;
	private long lastAppendTime;
	//private int dataSize; //FIXME: This field should be removed.
	private boolean dirty;
	
	private byte[] data;
	private final static int maxBlockSize = Constants.dataBlockSizeBytes;
	
	DataBlock(long uuidHigh, long uuidLow){
		this.uuidLow = uuidLow;
		this.uuidHigh = uuidHigh;
		
		createTime = System.currentTimeMillis();
		data = new byte[maxBlockSize];
		
		firstAppendTime = -1;
		lastAppendTime = -1;
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
		
		/*int capacity = capacity();
		if (capacity == 0)
			return 0;*/
		
		int offsetInBlock = (int)(offsetInFile % Constants.dataBlockSizeBytes);
		int capacity = Constants.dataBlockSizeBytes - offsetInBlock;
		
		long time = System.currentTimeMillis();
		int bytes = length <= capacity ? length : capacity;
		
		for(int i=0; i<bytes; i++){
			this.data[offsetInBlock] = data[i+offset];
			offsetInBlock++;
		}
		
		lastAppendTime = time;
		
		if (offsetInBlock == length || firstAppendTime == -1)
			firstAppendTime = time;
		
		dirty = true;
		
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
		
		long time = System.currentTimeMillis();
		lastAppendTime = time;
		if (offsetInBlock == 0 || firstAppendTime == -1)
			firstAppendTime = time;
		
		dirty = true;
		
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
		
		long time = System.currentTimeMillis();
		lastAppendTime = time;
		if (offsetInBlock == 0 || firstAppendTime == -1)
			firstAppendTime = time;
		
		dirty = true;
		
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
	
	/**
	 * @return Returns the number of bytes appended in this block. 
	 */
	/*synchronized int size(){
		return dataSize;
	}*/
	
	/*synchronized int capacity(){
		return maxBlockSize - dataSize;
	}*/
	
	long creationTime(){
		return createTime;
	}
	
	long firstAppendTime(){
		return firstAppendTime;
	}
	
	long lastAppendTime(){
		return lastAppendTime;
	}
	
	boolean hasByte(long offsetInFile){
		return false;
	}
	
	boolean dirty(){
		return dirty;
	}
	
	void clear(){
		dirty = false;
	}
	
	long uuidHigh(){
		return uuidHigh;
	}
	
	long uuidLow(){
		return uuidLow;
	}
	
	byte[] data(){
		return data;
	}
	
	/*FileOffset fileOffset(){
		return new FileOffset(offsetInFile, firstAppendTime);
	}*/
}
