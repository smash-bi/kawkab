package kawkab.fs.core;

import kawkab.fs.commons.Commons;
import kawkab.fs.commons.Configuration;
import kawkab.fs.core.Filesystem.FileMode;
import kawkab.fs.core.exceptions.*;

import java.io.IOException;

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
