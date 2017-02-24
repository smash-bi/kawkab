package kawkab.fs.api;

import kawkab.fs.commons.Constants;
import kawkab.fs.core.BlockMetadata;
import kawkab.fs.core.FileIndex;
import kawkab.fs.core.exceptions.InvalidFileOffsetException;
import kawkab.fs.core.exceptions.MaxFileSizeExceededException;
import kawkab.fs.core.exceptions.OutOfMemoryException;

public final class FileHandle {
	private String filename;
	private FileOptions options;
	private FileIndex fileIndex;
	private BlockMetadata currentBlock;
	private long readOffsetInFile;
	
	public FileHandle(String filename, FileOptions options, FileIndex fileIndex){
		this.filename = filename;
		this.options = options;
		this.fileIndex = fileIndex;
	}
	
	/**
	 * Reads data into the buffer
	 * @param buffer
	 * @param length Number of bytes to read from the file
	 * @return Number of bytes read from the file
	 */
	public int read(byte[] buffer, int length){
		int remaining = length;
		int read = 0;
		
		while(remaining > 0) {
			if (currentBlock == null || !currentBlock.hasByte(readOffsetInFile)){
				try {
					currentBlock = fileIndex.getByFileOffset(readOffsetInFile);
				} catch (InvalidFileOffsetException e) {
					e.printStackTrace();
					return read;
				}
			}
			
			if (currentBlock == null) //This means our byteOffset is out of the file region.
				break;
			
			/*if (!currentBlock.closed()){
				return read;
			}*/
			
			long lastOffset = currentBlock.offset() + Constants.defaultBlockSize;
			
			int toRead = (int)(readOffsetInFile+remaining <= lastOffset ? remaining : lastOffset - readOffsetInFile);
			int bytes;
			try {
				bytes = currentBlock.read(buffer, read, toRead, readOffsetInFile);
			} catch (InvalidFileOffsetException e) {
				e.printStackTrace();
				return read;
			}
			
			read += bytes;
			remaining -= bytes;
			readOffsetInFile += bytes;
		}
		
		return read;
	}
	
	/**
	 * Seek the read pointer to the absolute bytes byteOffset in the file
	 * @param byteOffset
	 */
	public void seekBytes(long byteOffset){
		readOffsetInFile = byteOffset;
	}
	
	/**
	 * Seek the read pointer to the first byte of the data block that contains the timestamp.
	 * @param tiemstamp
	 */
	public void seekTime(long tiemstamp){
		/*
		  - How to perform the read operation using the timestamp?
		  - Move the read pointer to the first byte of the block that contains the timestamp?
		  - What if the pointer does not lies within any block?
		    a) Move the read pointer to the first byte of the previous block?
		    b) Or move the read pointer to the first byte of the next block?
		    c) Or do not move the read pointer and return an error?
		  - What if the timestamp is greater than the last append time?
		    - Should return an error in the seekTime function?
		 */
	}
	
	/**
	 * Move the read pointer to numBytes relative to its current position
	 * @param bytes number of bytes to move the read pointer
	 */
	public void relativeSeek(long numBytes){
		readOffsetInFile = readOffsetInFile + numBytes;
	}
	
	/**
	 * Append data at the end of the file
	 * @param data Data to append
	 * @param offset Offset in the data array from where to start copying data
	 * @param length Number of bytes to write from the data array
	 * @return Index of the data appended to the file. The caller can use 
	 * dataIndex.timestamp() to refer to the data just written. 
	 * @throws OutOfMemoryException 
	 */
	public int append(byte[] data, int offset, int length) throws OutOfMemoryException, MaxFileSizeExceededException{
		return fileIndex.append(data, offset, length);
	}
}
