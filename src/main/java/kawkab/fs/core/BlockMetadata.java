package kawkab.fs.core;

import kawkab.fs.core.exceptions.InvalidFileOffsetException;

public class BlockMetadata {
	private long blockNumber; //starts from 0.
	private long offsetInFile; //offset of the first data byte relative to the start of the file
	private boolean closed;
	private final long createTime;
	private long lastAppendTime;
	private BlockLocation location;
	private DataBlock block;
	
	public BlockMetadata(long blockNumber, long offsetInFile){
		System.out.println("Creating block " + blockNumber);
		
		block = new DataBlock(offsetInFile);
		
		//FIXME: This value should be set at the first successful append in the block
		this.offsetInFile = offsetInFile;
		this.blockNumber = blockNumber;
		
		//FIXME: Should it not be a unique timestamp for each append? How to handle nanoSecond timestamps?
		createTime = System.currentTimeMillis();
	}
	
	public synchronized int append(byte[] data, int offset, int length){
		if (closed())
			return 0; //TODO: Make it DataBlockClosedException.
		
		int bytes =  block.append(data, offset, length);
		lastAppendTime = System.currentTimeMillis();
		
		if (capacity() == 0)
			close();
		
		return bytes;
	}
	
	/**
	 * @param buffer output buffer
	 * @param offset offset in the file from where to read data
	 * @param length length of data to be read
	 * @return number of bytes read
	 * @throws IncorrectOffsetException 
	 */
	public synchronized int read(byte[] buffer, int bufferOffset, int length, long offsetInFile) throws InvalidFileOffsetException{
		return block.read(buffer, bufferOffset, length, offsetInFile);
	}
	
	public long index(){
		return blockNumber;
	}
	
	public synchronized int capacity(){
		return block.capacity();
	}
	
	public synchronized void close(){
		closed = true;
	}
	
	public boolean closed(){
		return closed;
	}
	
	public long offset(){
		return offsetInFile;
	}
	
	public int size(){
		return block.size();
	}
	
	public long creationTime(){
		return createTime;
	}
	
	public long lastAppendTime(){
		return lastAppendTime;
	}
	
	public boolean hasByte(long offsetInFile){
		return false;
	}
}
