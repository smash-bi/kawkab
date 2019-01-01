package kawkab.fs.api;

import java.io.IOException;

import kawkab.fs.commons.Constants;
import kawkab.fs.core.BlockID;
import kawkab.fs.core.Cache;
import kawkab.fs.core.Filesystem.FileMode;
import kawkab.fs.core.Inode;
import kawkab.fs.core.InodesBlock;
import kawkab.fs.core.InodesBlockID;
import kawkab.fs.core.exceptions.InvalidFileModeException;
import kawkab.fs.core.exceptions.InvalidFileOffsetException;
import kawkab.fs.core.exceptions.KawkabException;
import kawkab.fs.core.exceptions.MaxFileSizeExceededException;
import kawkab.fs.core.exceptions.OutOfMemoryException;

public final class FileHandle {
	private long inumber;
	private FileMode fileMode;
	private long readOffsetInFile;
	private static Cache cache;
	
	static { //Using static block to catch the exception during initialization
		try {
			cache = Cache.instance();
		} catch (IOException e) { //FIXME: Handle the exception properly
			e.printStackTrace();
		}
	}
	
	public FileHandle(long inumber, FileMode mode){
		this.inumber = inumber;
		this.fileMode = mode;
	}
	
	/**
	 * Reads data into the buffer
	 * @param buffer
	 * @param length Number of bytes to read from the file.
	 * @return Number of bytes read from the file
	 * @throws IOException 
	 * @throws KawkabException 
	 * @throws InterruptedException 
	 */
	public synchronized int read(byte[] buffer, int length) throws IOException, IllegalArgumentException, KawkabException, InterruptedException {
		/*1. Borrow InodesBlock from cache
		    2. Acquire InodesBlock lock
		      3. Get the pointer to the dataBlock or the indirectBlock
		      4. Get the current fileSize, which is the maximum byte that a read operation
		         can serve.
		    5. Release InodesBlock lock
		  6. Return inodesBlock to the cache
		  7. Borrow indirectBlock from the cache
		    8. Get the pointer to the dataBlock
		  9. Return the indirectBlock to the cache
		  10. Borrow DataBlock from the cache
		    11. Read data
		  12. Return DataBlock to the cache*/
		
		if (length > buffer.length)
			throw new IllegalArgumentException("Read data size is greater than the given buffer size.");
		
		int blockIndex = (int)(inumber / Constants.inodesPerBlock);
		//System.out.println("[FH] inodeBlock: " + blockIndex);
		BlockID id = new InodesBlockID(blockIndex);
		
		Inode inode = null;
		long fileSize = 0;
		InodesBlock block = null;
		try {
			block = (InodesBlock)cache.acquireBlock(id, false);
			block.lock(); // To read the current file size, which can be updated by a concurrent writer
			inode = block.getInode(inumber);
			fileSize = inode.fileSize();
		} finally {
			if (block != null) {
				block.unlock();
				cache.releaseBlock(block.id());
			}
		}
		
		if (readOffsetInFile + length > fileSize)
			throw new IllegalArgumentException(String.format(
					"Read length exceeds file length: Read offset=%d, read length=%d, file size=%d.",
					readOffsetInFile, length, fileSize));
		
		int bytesRead = 0;
		try {
			bytesRead = inode.read(buffer, length, readOffsetInFile);
		} catch (InvalidFileOffsetException e) {
			e.printStackTrace();
			return bytesRead;
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
			return bytesRead;
		}
		
		readOffsetInFile += bytesRead;
		
		return bytesRead;
	}
	
	/**
	 * Seek the read pointer to the byteOffset bytes in the file
	 * @param byteOffset
	 */
	public synchronized void seekBytes(long byteOffset){
		readOffsetInFile = byteOffset;
	}
	
	/**
	 * Seek the read pointer to the first byte of the first data block that is at or before the time timestamp.
	 * @param tiemstamp
	 * @return Offset of the block where the read pointer is moved to, or null if the data block
	 *          is not found.
	 */
	/*public FileOffset seekBeforeTime(long timestamp){
		if (timestamp <= 0)
			return null; //FIXME: Make it an exception.
		
		DataBlock block = fileIndex.getByTime(timestamp, true);
		if (block == null)
			return null;
		
		FileOffset offset = block.fileOffset();
		readOffsetInFile = offset.offsetInFile();
		return offset;
	}*/
	
	/**
	 * Seek the read pointer to the first byte of the first data block that is at or after the time timestamp.
	 * @param tiemstamp
	 * @return Offset of the block where the read pointer is moved to, or null if the data block
	 *          is not found.
	 */
	/*public FileOffset seekAfterTime(long timestamp){
		DataBlock block = fileIndex.getByTime(timestamp, false);
		if (block == null)
			return null;
		
		FileOffset offset = block.fileOffset();
		readOffsetInFile = offset.offsetInFile();
		return offset;
	}*/
	
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
	
	/*public DataOffset offsetAndDataLength(long time1, long time2){
		if (time1 < 0) time1 = 0;
		if (time2 < 0) time2 = 0;
			
		if (time1 > time2){ //Swap times
			time1 = time1 ^ time2;
			time2 = time1 ^ time2;
			time1 = time1 ^ time2;
		}
		
		DataBlock blockLeft = fileIndex.getByTime(time1, true);
		if (blockLeft == null) return null;
		DataBlock blockRight = fileIndex.getByTime(time2, false);
		if (blockRight == null) return null;
		
		long size = blockRight.offset() + blockRight.size() - blockLeft.offset();
		return new DataOffset(blockLeft.offset(), size);
	}*/
	
	/**
	 * Move the read pointer to numBytes relative to its current position
	 * @param bytes number of bytes to move the read pointer
	 */
	public synchronized void relativeSeek(long numBytes){
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
	 * @throws InvalidFileOffsetException 
	 * @throws IOException 
	 * @throws KawkabException 
	 * @throws InterruptedException 
	 */
	public synchronized int append(byte[] data, int offset, int length) throws OutOfMemoryException, 
									MaxFileSizeExceededException, InvalidFileOffsetException, 
									IOException, KawkabException, InterruptedException{
		/*- File Append
		  1. Borrow InodesBlock from the cache
		    2. Acquire InodesBlock lock
		      3. If file is currently being updated, wait on an updateCondition
		      4. Otherwise, mark the file as being updated
		    5. Release InodesBlock lock
		    6. Allocate and borrow necessary indirectBlocks from the cache
		    7. Allocate and borrow dataBlocks from the cache
		      8. update data
		      9. Mark datablock as dirty
		    10. Return DataBlock to the cache
		    11. Mark indirectBlocks dirty as required
		    12. Return indirectBlocks to the cache
		    13. Acquire InodesBlock lock
		      14. Update fileSize and other information
		    16. Mark inodesBlock as dirty
		    15. Release InodesBlock lock
		  17. Return inodesBlock to the cache*/
		if (fileMode != FileMode.APPEND) {
			throw new InvalidFileModeException();
		}
		
		/*int len = 0;
		int inodesBlockNum = (int)(inumber / Constants.inodesPerBlock);
		try (InodesBlock block = cache.getInodesBlock(inodesBlockNum)) {
			len = block.append(inumber, data, offset, length);
		}
		return len;*/
		
		int appendedBytes = 0;
		int inodesBlockIdx = (int)(inumber / Constants.inodesPerBlock);
		BlockID id = new InodesBlockID(inodesBlockIdx);
		
		InodesBlock inodesBlock = null;
		try {
			inodesBlock = (InodesBlock)cache.acquireBlock(id, false);
			//block.lock();
			//try {
				//TODO: 3. If file is currently being updated, wait on an updateCondition
			    //TODO: 4. Otherwise, mark the file as being updated
			//} finally {
			//	block.unlock();
			//}
			
			Inode inode = inodesBlock.getInode(inumber);
			appendedBytes = inode.append(data, offset, length);
			
			if (appendedBytes > 0) {
				try {
					inodesBlock.lock();
					inode.updateSize(appendedBytes);
					inodesBlock.markLocalDirty();
				} finally {
					inodesBlock.unlock();
				}
			}
		} finally {
			if (inodesBlock != null) {
				cache.releaseBlock(inodesBlock.id());
			}
		}
		
		return appendedBytes;
	}
	
	/**
	 * @return Returns file size in bytes.
	 * @throws KawkabException 
	 * @throws InterruptedException 
	 */
	public synchronized long size() throws KawkabException, InterruptedException{
		int inodesBlockIdx = (int)(inumber / Constants.inodesPerBlock);
		long size = 0;
		BlockID id = new InodesBlockID(inodesBlockIdx);
		
		InodesBlock block = null;
		try {
			block = (InodesBlock) cache.acquireBlock(id, false);
			size = block.fileSize(inumber);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (block != null) {
				cache.releaseBlock(block.id());
			}
		}
		
		return size;
	}
	
	/**
	 * @return Returns the current value of the read pointer; the file offset in bytes from where 
	 * the read function starts reading. 
	 */
	public synchronized long readOffset(){
		return readOffsetInFile;
	}
	
	public long inumber() { // For debugging only
		return inumber;
	}
	
	public FileMode mode() {
		return fileMode;
	}
}
