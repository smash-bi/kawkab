package kawkab.fs.core;

import java.io.IOException;

import kawkab.fs.commons.Configuration;
import kawkab.fs.core.Filesystem.FileMode;
import kawkab.fs.core.exceptions.InvalidFileModeException;
import kawkab.fs.core.exceptions.InvalidFileOffsetException;
import kawkab.fs.core.exceptions.KawkabException;
import kawkab.fs.core.exceptions.MaxFileSizeExceededException;
import kawkab.fs.core.exceptions.OutOfMemoryException;

/**
 * TODO: Store InodeBlocks in a separate table than the cache.
 * TODO: Remove the InodeBlocks from the table when necessary
 * TODO: Keep a reference count of InodeBlocks
 */

public final class FileHandle {
	private final long inumber;
	private final FileMode fileMode;
	private static Cache cache;
	private final static int inodesPerBlock;
	private Inode inode;
	private InodesBlock inodesBlock;
	
	static {
		Configuration conf = Configuration.instance();
		inodesPerBlock = conf.inodesPerBlock;
		cache = Cache.instance();
	}
	
	public FileHandle(long inumber, FileMode mode) throws IOException, KawkabException{
		this.inumber = inumber;
		this.fileMode = mode;
		
		int inodesBlockIdx = (int)(inumber / inodesPerBlock);
		BlockID id = new InodesBlockID(inodesBlockIdx);
		
		InodesBlock inb = null;
		try {
			inb = (InodesBlock)cache.acquireBlock(id);
			inb.loadBlock();
			
		} catch (IOException | KawkabException e) {
			inode = null;
			inodesBlock = null;
			throw e;
		}
		
		inodesBlock = inb;
		inode = inodesBlock.getInode(inumber);
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
	public synchronized int read(byte[] buffer, long readOffsetInFile, int length) throws IOException, IllegalArgumentException, KawkabException, InterruptedException {
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
		
		//int blockIndex = (int)(inumber / inodesPerBlock);
		//System.out.println("[FH] inodeBlock: " + blockIndex);
		//BlockID id = new InodesBlockID(blockIndex);
		
		//Inode inode = null;
		//long fileSize = 0;
		//InodesBlock block = null;
		//try {
			//block = (InodesBlock)cache.acquireBlock(id);
			//block.lock(); // To read the current file size, which can be updated by a concurrent writer
			//inode = block.getInode(inumber);
			//fileSize = inode.fileSize();
		//} finally {
			//if (block != null) {
			//	block.unlock();
			//	cache.releaseBlock(block.id());
			//}
		//}
		
		long fileSize = inode.fileSize();
		
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
	 * Append data at the end of the file
	 * @param data Data to append
	 * @param offset Offset in the data array from where to start copying data
	 * @param length Number of bytes to write from the data array
	 * @return Number of bytes appended 
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
		
		if (inodesBlock == null) {
			throw new KawkabException("The file handle is closed. Open the file again to get the new handle.");
		}
		
		int appendedBytes = inode.appendBuffered(data, offset, length);
		inodesBlock.markLocalDirty();
		
		return appendedBytes;
	}
	
	/**
	 * @return Returns file size in bytes.
	 * @throws KawkabException 
	 * @throws InterruptedException 
	 */
	public synchronized long size() throws KawkabException, InterruptedException{
		return inodesBlock.fileSize(inumber);
	}
	
	public long inumber() { // For debugging only
		return inumber;
	}
	
	public FileMode mode() {
		return fileMode;
	}
	
	public synchronized void close() throws KawkabException {
		
		if (inodesBlock != null) {
			cache.releaseBlock(inodesBlock.id());
		}
		
		inode = null;
		inodesBlock = null;
		
		//TODO: Update openFiles table.
	}
}
