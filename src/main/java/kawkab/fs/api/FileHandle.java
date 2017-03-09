package kawkab.fs.api;

import kawkab.fs.commons.Constants;
import kawkab.fs.core.BlockMetadata;
import kawkab.fs.core.FileIndex;
import kawkab.fs.core.FileOffset;
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
			
			long lastOffset = currentBlock.offset() + Constants.dataBlockSizeBytes;
			
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
	 * Seek the read pointer to the byteOffset bytes in the file
	 * @param byteOffset
	 */
	public void seekBytes(long byteOffset){
		readOffsetInFile = byteOffset;
	}
	
	/**
	 * Seek the read pointer to the first byte of the first data block that is at or before the time timestamp.
	 * @param tiemstamp
	 * @return Offset of the block where the read pointer is moved to, or null if the data block
	 *          is not found.
	 */
	public FileOffset seekBeforeTime(long timestamp){
		if (timestamp <= 0)
			return null; //FIXME: Make it an exception.
		
		BlockMetadata block = fileIndex.getByTime(timestamp, true);
		if (block == null)
			return null;
		
		FileOffset offset = block.fileOffset();
		readOffsetInFile = offset.offsetInFile();
		return offset;
	}
	
	/**
	 * Seek the read pointer to the first byte of the first data block that is at or after the time timestamp.
	 * @param tiemstamp
	 * @return Offset of the block where the read pointer is moved to, or null if the data block
	 *          is not found.
	 */
	public FileOffset seekAfterTime(long timestamp){
		BlockMetadata block = fileIndex.getByTime(timestamp, false);
		if (block == null)
			return null;
		
		FileOffset offset = block.fileOffset();
		readOffsetInFile = offset.offsetInFile();
		return offset;
	}
	
	/**
	 * Returns the offset and the number of bytes between two time index boundaries.
	 * If time1 and time2 lie in blocks B1 and B2, the function returns the offset of B1 and the
	 * size = B2.offset + B2.size - B1.offset. 
	 * 
	 * The function returns the offset at the start of the 
	 * block B1 that contains T1 and the size of the data is from start of B1 to the end of the block
	 * B2 that contains T2.
	 * 
	 * This function does not change the read position in file.
	 * 
	 * @param time1 The start of the boundary that is at or before time1
	 * @param time2 The end of the boundary that is at or after time
	 * @return DataOffset that contains the offset of the data relative to start of the file and
	 *          the size of data. Returns null if data is not found within the given time boundaries.
	 */
	
	public DataOffset offsetAndDataLength(long time1, long time2){
		if (time1 < 0) time1 = 0;
		if (time2 < 0) time2 = 0;
			
		if (time1 > time2){ //Swap times
			time1 = time1 ^ time2;
			time2 = time1 ^ time2;
			time1 = time1 ^ time2;
		}
		
		BlockMetadata blockLeft = fileIndex.getByTime(time1, true);
		if (blockLeft == null) return null;
		BlockMetadata blockRight = fileIndex.getByTime(time2, false);
		if (blockRight == null) return null;
		
		long size = blockRight.offset() + blockRight.size() - blockLeft.offset();
		return new DataOffset(blockLeft.offset(), size);
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
