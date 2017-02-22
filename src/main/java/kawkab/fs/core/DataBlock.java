package kawkab.fs.core;

import kawkab.fs.commons.Constants;
import kawkab.fs.core.exceptions.InvalidFileOffsetException;

public class DataBlock {
	private final long offsetInFile; //offset of the first data byte relative to the start of the file
	private int  dataSize;
	private byte[] data;
	private final static int maxBlockSize = Constants.defaultBlockSize;
	
	protected DataBlock(long offsetInFile){
		this.offsetInFile = offsetInFile;
		this.data = new byte[maxBlockSize];
	}
	
	public synchronized int append(byte[] data, int offset, int length){
		int capacity = capacity();
		int appendSize = length <= capacity ? length : capacity;
		
		for(int i=0; i<appendSize; i++){
			/*if (this.data.length <= dataSize+i) {
				System.out.println(this.data.length);
				System.out.println(dataSize);
				System.out.println(i);
				System.out.println(length);
				System.out.println(appendSize);
			}*/
			
			this.data[dataSize] = data[i+offset];
			dataSize++;
		}
		return appendSize;
	}
	
	/**
	 * @param buffer output buffer
	 * @param bufferOffset offset in the buffer from where data will be copied
	 * @param length length of data to be read
	 * @param offset offset in the file from where to read data
	 * @return number of bytes read
	 * @throws IncorrectOffsetException 
	 */
	public synchronized int read(byte[] buffer, int bufferOffset, int length, long offsetInFile) throws InvalidFileOffsetException{
		int blockOffset = (int)(offsetInFile - this.offsetInFile);
		
		if (blockOffset > dataSize || blockOffset < 0) {
			throw new InvalidFileOffsetException(
					String.format("Given offset %d is outside of the block: %d - %d",
					offsetInFile, this.offsetInFile, this.offsetInFile+dataSize));
		}
		
		int readSize = blockOffset+length <= dataSize ? length : dataSize-blockOffset;
		for(int i=0; i<readSize; i++){
			buffer[bufferOffset+i] = this.data[blockOffset+i];
		}
		
		return readSize;
	}
	
	public synchronized int capacity(){
		return maxBlockSize - dataSize;
	}
	
	public synchronized int size(){
		return dataSize;
	}
}
