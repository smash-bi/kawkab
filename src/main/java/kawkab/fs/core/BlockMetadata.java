package kawkab.fs.core;

import kawkab.fs.core.exceptions.InvalidFileOffsetException;

public class BlockMetadata {
	private final long blockNumber; //starts from 0.
	private final long offsetInFile; //offset of the first data byte relative to the start of the file
	private final long createTime;
	private boolean closed;
	private long firstAppendTime = -1;
	private long lastAppendTime = -1;
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
		
		long time = System.currentTimeMillis();
		int bytes =  block.append(data, offset, length);
		lastAppendTime = time;
		
		if (size() == length)
			firstAppendTime = time;
		
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
	
	/**
	 * @return Returns the file offset of the first byte in the block 
	 */
	public long offset(){
		return offsetInFile;
	}
	
	public int size(){
		return block.size();
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
	
	public FileOffset fileOffset(){
		return new FileOffset(offsetInFile, firstAppendTime);
	}
}
