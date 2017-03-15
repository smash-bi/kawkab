package kawkab.fs.core;

import java.nio.ByteBuffer;

import kawkab.fs.commons.Constants;
import kawkab.fs.core.exceptions.InvalidFileOffsetException;

public class BlockMetadata {
	private final long uuidLow;
	private final long uuidHigh;
	//private final long blockNumber; //starts from 0.
	//private final long offsetInFile; //offset of the first data byte relative to the start of the file
	private final long createTime;
	private long firstAppendTime = -1;
	private long lastAppendTime = -1;
	private int dataSize; //FIXME: This field should be removed.
	private boolean dirty;
	
	private byte[] data;
	private final static int maxBlockSize = Constants.dataBlockSizeBytes;
	
	public BlockMetadata(long uuidHigh, long uuidLow){
		System.out.println("Creating a new data block.");
		
		this.uuidLow = uuidLow;
		this.uuidHigh = uuidHigh;
		
		createTime = System.currentTimeMillis();
		data = new byte[maxBlockSize];
	}
	
	public synchronized int append(byte[] data, int offset, int length){
		assert offset >= 0;
		assert offset < data.length;
		
		int capacity = capacity();
		if (capacity == 0)
			return 0;
		
		long time = System.currentTimeMillis();
		int bytes = length <= capacity ? length : capacity;
		
		for(int i=0; i<bytes; i++){
			this.data[dataSize] = data[i+offset];
			dataSize++;
		}
		
		lastAppendTime = time;
		
		if (dataSize == length)
			firstAppendTime = time;
		
		dirty = true;
		
		return bytes;
	}
	
	public synchronized int appendLong(long data){
		if (capacity() < 8)
			return 0;
		
		ByteBuffer buffer = ByteBuffer.wrap(this.data);
		buffer.position(dataSize);
		buffer.putLong(data);
		dataSize += 8;
		
		long time = System.currentTimeMillis();
		lastAppendTime = time;
		if (dataSize == 8)
			firstAppendTime = time;
		
		dirty = true;
		
		return 8;
	}
	
	public synchronized long readLong(long fileOffsetBytes) throws InvalidFileOffsetException{
		int blockOffset = (int)(fileOffsetBytes % Constants.dataBlockSizeBytes);
		
		if (blockOffset >= dataSize)
			throw new InvalidFileOffsetException(
					String.format("File Offset %d is invalid. Block size is %d.", fileOffsetBytes, dataSize));
		
		ByteBuffer buffer = ByteBuffer.wrap(this.data);
		buffer.position(blockOffset);
		return buffer.getLong();
	}
	
	public synchronized int appendInt(int data){
		if (capacity() < 4)
			return 0;
		
		ByteBuffer buffer = ByteBuffer.wrap(this.data);
		buffer.position(dataSize);
		buffer.putInt(data);
		dataSize += 4;
		
		long time = System.currentTimeMillis();
		lastAppendTime = time;
		if (dataSize == 4)
			firstAppendTime = time;
		
		dirty = true;
		
		return 4;
	}
	
	public synchronized int readInt(long fileOffsetBytes) throws InvalidFileOffsetException{
		int blockOffset = (int)(fileOffsetBytes % Constants.dataBlockSizeBytes);
		
		if (blockOffset >= dataSize)
			throw new InvalidFileOffsetException(
					String.format("File Offset %d is invalid. Block size is %d.", fileOffsetBytes, dataSize));
		
		ByteBuffer buffer = ByteBuffer.wrap(this.data);
		buffer.position(blockOffset);
		return buffer.getInt();
	}
	
	/**
	 * @param buffer output buffer
	 * @param bufferOffset offset in the buffer from where data will be copied
	 * @param length length of data to be read
	 * @param offset offset in the file from where to read data
	 * @return number of bytes read
	 * @throws IncorrectOffsetException 
	 */
	public synchronized int read(byte[] buffer, int bufferOffset, int length, long fileOffsetBytes) throws InvalidFileOffsetException{
		int blockOffset = (int)(fileOffsetBytes % Constants.dataBlockSizeBytes);
		
		if (blockOffset > dataSize || blockOffset < 0) {
			throw new InvalidFileOffsetException(
					String.format("Given file offset %d is outside of the block. Data in this block is %d bytes",
					fileOffsetBytes, dataSize));
		}
		
		int readSize = blockOffset+length <= dataSize ? length : dataSize-blockOffset;
		for(int i=0; i<readSize; i++){
			buffer[bufferOffset+i] = this.data[blockOffset+i];
		}
		
		return readSize;
	}
	
	public synchronized void fill(){
		long time = System.currentTimeMillis();
		if (dataSize == 0)
			firstAppendTime = time;
		lastAppendTime = time;
		dataSize = maxBlockSize;
	}
	
	/**
	 * @return Returns the number of bytes appended in this block. 
	 */
	public synchronized int size(){
		return dataSize;
	}
	
	public synchronized int capacity(){
		return maxBlockSize - dataSize;
	}
	
	public long creationTime(){
		return createTime;
	}
	
	public long firstAppendTime(){
		return firstAppendTime;
	}
	
	public long lastAppendTime(){
		return lastAppendTime;
	}
	
	public boolean hasByte(long offsetInFile){
		return false;
	}
	
	public boolean dirty(){
		return dirty;
	}
	
	public void clear(){
		dirty = false;
	}
	
	public long uuidHigh(){
		return uuidHigh;
	}
	
	public long uuidLow(){
		return uuidLow;
	}
	
	public byte[] data(){
		return data;
	}
	
	/*public FileOffset fileOffset(){
		return new FileOffset(offsetInFile, firstAppendTime);
	}*/
}
