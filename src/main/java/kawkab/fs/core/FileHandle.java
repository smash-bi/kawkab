package kawkab.fs.core;

import kawkab.fs.api.Record;
import kawkab.fs.commons.Commons;
import kawkab.fs.commons.Configuration;
import kawkab.fs.core.Filesystem.FileMode;
import kawkab.fs.core.exceptions.*;

import java.io.IOException;
import java.util.List;

/**
 * TODO: Store InodeBlocks in a separate table than the cache.
 * TODO: Remove the InodeBlocks from the table when necessary
 * TODO: Keep a reference count of InodeBlocks
 */

public final class FileHandle {
	private final long inumber;
	private final FileMode fileMode;
	private final static Cache cache;
	private final static int inodesPerBlock;	// Used in accessing the inode of this file when on the non-primary node
	private Inode inode;	// Not final because we set it to null in the close() in order to free the memory
	private InodesBlock inodesBlock;	// Not final because we set it to null in the close() in order to free the memory
	private final boolean onPrimaryNode;	//Indicates whether this file is opened on its primary node or not
	private final static LocalStoreManager localStore;	// FIXME: Isn't it a bad design to access localStore from a file handle?

	static {
		Configuration conf = Configuration.instance();
		inodesPerBlock = conf.inodesPerBlock;
		cache = Cache.instance();
		localStore = LocalStoreManager.instance();
	}

	public FileHandle(long inumber, FileMode mode) throws IOException, KawkabException{
		this.inumber = inumber;
		this.fileMode = mode;
		onPrimaryNode = Configuration.instance().thisNodeID == Commons.primaryWriterID(inumber); //Is this reader or writer on the primary node?

		int inodesBlockIdx = (int) (inumber / inodesPerBlock);
		BlockID id = new InodesBlockID(inodesBlockIdx);
		
		InodesBlock inb = null;
		try {
			inb = (InodesBlock) cache.acquireBlock(id);
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
	public synchronized int read(byte[] buffer, long readOffsetInFile, int length) throws
			IOException, IllegalArgumentException, KawkabException, InvalidFileOffsetException {
		if (length > buffer.length || length < 0)
			throw new IllegalArgumentException("Read length is negative or greater than the given buffer size.");

		long fileSize = 0;
		InodesBlock inb = null;
		Inode inode;
		int bytesRead = 0;

		try {
			if (onPrimaryNode) {
				if (inodesBlock == null) {
					throw new KawkabException("The file handle is closed. Open the file again to get the new handle.");
				}
				inb = this.inodesBlock;
				inode = this.inode;
			} else {
				int blockIndex = (int)(inumber / inodesPerBlock);
				BlockID id = new InodesBlockID(blockIndex);
				inb = (InodesBlock) cache.acquireBlock(id);
				inb.loadBlock();
				inode = inb.getInode(inumber);
			}

			fileSize = inode.fileSize();

			if (readOffsetInFile + length > fileSize)
				throw new IllegalArgumentException(String.format(
						"Read length exceeds file length: Read offset=%d, read length=%d, file size=%d.",
						readOffsetInFile, length, fileSize));

			bytesRead = inode.read(buffer, length, readOffsetInFile);
		} finally {
			if (!onPrimaryNode && inb != null) {
				cache.releaseBlock(inb.id());
			}
		}

		return bytesRead;
	}

	public synchronized List<Record> readRecords(final long minTS, final long maxTS, final Record recFactory) throws KawkabException, IOException {
		InodesBlock inb = null;
		Inode inode;

		try {
			if (onPrimaryNode) {
				if (inodesBlock == null) {
					throw new KawkabException("The file handle is closed. Open the file again to get the new handle.");
				}
				inb = this.inodesBlock;
				inode = this.inode;
			} else {
				int blockIndex = (int)(inumber / inodesPerBlock);
				BlockID id = new InodesBlockID(blockIndex);
				inb = (InodesBlock) cache.acquireBlock(id);
				inb.loadBlock();
				inode = inb.getInode(inumber);
			}

			return inode.readAll(minTS, maxTS, recFactory);
		} finally {
			if (!onPrimaryNode && inb != null) {
				cache.releaseBlock(inb.id());
			}
		}
	}
	
	/**
	 * Loads the dstRecord from the key (timestamp) location in the file.
	 *
	 * @param dstRecord Output field
	 * @param key Exact match timestamp
	 *
	 * @return Whether the record is found and loaded
	 * @throws IOException
	 * @throws KawkabException
	 * @throws InterruptedException
	 */
	public synchronized boolean recordAt(final Record dstRecord, final long key) throws
			IOException, RecordNotFoundException, KawkabException {
		
		InodesBlock inb = null;
		Inode inode;
		
		try {
			if (onPrimaryNode) {
				if (inodesBlock == null) {
					throw new KawkabException("The file handle is closed. Open the file again to get the new handle.");
				}
				inb = this.inodesBlock;
				inode = this.inode;
			} else {
				int blockIndex = (int)(inumber / inodesPerBlock);
				BlockID id = new InodesBlockID(blockIndex);
				inb = (InodesBlock) cache.acquireBlock(id);
				inb.loadBlock();
				inode = inb.getInode(inumber);
			}
			
			return inode.read(dstRecord, key);
		} finally {
			if (!onPrimaryNode && inb != null) {
				cache.releaseBlock(inb.id());
			}
		}
	}
	
	/**
	 * Loads the dstRecord from the record number recordNum in the file.
	 *
	 * @param recordNum the record number in the file, 1 being the first record. recNum should be greater than 0.
	 *
	 * @return Whether the record is found and loaded
	 *
	 * @throws IOException
	 * @throws KawkabException
	 * @throws InterruptedException
	 * @throws RecordNotFoundException if the record does not exist in the file
	 * @throws InvalidFileOffsetException if the recordNum is less than 1
	 */
	public synchronized boolean recordNum(final Record dstRecord, final long recordNum) throws
			IOException, KawkabException, RecordNotFoundException, InvalidFileOffsetException {
		
		if (recordNum <= 0)
			throw new InvalidFileOffsetException("Record number " + recordNum + " is invalid.");
		
		InodesBlock inb = null;
		Inode inode;
		
		try {
			if (onPrimaryNode) {
				if (inodesBlock == null) {
					throw new KawkabException("The file handle is closed. Open the file again to get the new handle.");
				}
				inb = this.inodesBlock;
				inode = this.inode;
			} else {
				int blockIndex = (int)(inumber / inodesPerBlock);
				BlockID id = new InodesBlockID(blockIndex);
				inb = (InodesBlock) cache.acquireBlock(id);
				inb.loadBlock();
				inode = inb.getInode(inumber);
			}
			
			return inode.readRecordN(dstRecord, recordNum);
		} finally {
			if (!onPrimaryNode && inb != null) {
				cache.releaseBlock(inb.id());
			}
		}
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
	
	public synchronized int append(byte[] data, int offset, int length) throws MaxFileSizeExceededException,
									IOException, KawkabException, InterruptedException{
		if (fileMode != FileMode.APPEND || !onPrimaryNode) {
			throw new InvalidFileModeException();
		}

		if (inodesBlock == null) {
			throw new KawkabException("The file handle is closed. Open the file again to get the new handle.");
		}
		
		int appendedBytes = inode.appendBuffered(data, offset, length);

		inodesBlock.markLocalDirty();
		localStore.store(inodesBlock);
		
		return appendedBytes;
	}
	
	// FIXME: Create a separate interface for binary and structured files. Binary append and record-based appends
	// are mutually exclusive. They should not be mixed in the same file.
	
	/**
	 * Append data at the end of the file and index with the given key
	 * @param record A single file-record
	 * @return Number of bytes appended
	 * dataIndex.timestamp() to refer to the data just written.
	 * @throws OutOfMemoryException
	 * @throws InvalidFileOffsetException
	 * @throws IOException
	 * @throws KawkabException
	 * @throws InterruptedException
	 */
	public synchronized int append(final Record record) throws MaxFileSizeExceededException,
			IOException, KawkabException, InterruptedException{
		if (fileMode != FileMode.APPEND || !onPrimaryNode) {
			throw new InvalidFileModeException();
		}
		
		if (inodesBlock == null) {
			throw new KawkabException("The file handle is closed. Open the file again to get the new handle.");
		}
		
		int appendedBytes = inode.appendBuffered(record);
		
		inodesBlock.markLocalDirty();
		localStore.store(inodesBlock);
		
		return appendedBytes;
	}

	/**
	 * @return Returns file size in bytes.
	 * @throws KawkabException 
	 * @throws InterruptedException 
	 */
	public synchronized long size() throws KawkabException {
		if (onPrimaryNode) {
			if (inodesBlock == null) {
				throw new KawkabException("The file handle is closed. Open the file again to get the new handle.");
			}

			return inode.fileSize();
		}

		int inodesBlockIdx = (int)(inumber / inodesPerBlock);
		long size = 0;
		BlockID id = new InodesBlockID(inodesBlockIdx);

		InodesBlock block = null;
		try {
			block = (InodesBlock) cache.acquireBlock(id);
			block.loadBlock();
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
	
	public long inumber() { // For debugging only
		return inumber;
	}
	
	public FileMode mode() {
		return fileMode;
	}
	
	synchronized void close() throws KawkabException {
		if (inodesBlock != null) {
			inode.releaseBuffer();
			cache.releaseBlock(inodesBlock.id());
		}

		inode = null;
		inodesBlock = null;
		
		//TODO: Update openFiles table.
	}
}
