package kawkab.fs.core;

import kawkab.fs.api.Record;
import kawkab.fs.commons.Commons;
import kawkab.fs.commons.Configuration;
import kawkab.fs.core.Filesystem.FileMode;
import kawkab.fs.core.exceptions.*;
import kawkab.fs.core.timerqueue.DeferredWorkReceiver;
import kawkab.fs.core.timerqueue.TimerQueueIface;
import kawkab.fs.core.timerqueue.TimerQueueItem;
import kawkab.fs.utils.TimeLog;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * TODO: Store InodeBlocks in a separate table than the cache.
 * TODO: Remove the InodeBlocks from the table when necessary
 * TODO: Keep a reference count of InodeBlocks
 */

public final class FileHandle implements DeferredWorkReceiver<InodesBlock> {
	private final long inumber;
	private final FileMode fileMode;
	private Inode inode; // Not final because we set it to null in the close() in order to free the memory
	private InodesBlock inodesBlock; // Not final because we set it to null in the close() in order to free the memory
	private final boolean onPrimaryNode; //Indicates whether this file is opened on its primary node or not
	private final TimerQueueIface fsQ;
	private TimerQueueItem<InodesBlock> inbAcquired;

	private final static Cache cache;
	private final static Clock clock = Clock.instance();
	private final static int inodesPerBlock;	// Used in accessing the inode of this file when on the non-primary node
	private final static LocalStoreManager localStore;	// FIXME: Isn't it a bad design to access localStore from a file handle?
	private final static int bufferTimeLimitMs = 5000;
	private final TimeLog rLog;
	private final TimeLog wLog;

	static {
		Configuration conf = Configuration.instance();
		inodesPerBlock = conf.inodesPerBlock;
		cache = Cache.instance();
		localStore = LocalStoreManager.instance();
	}

	public FileHandle(long inumber, FileMode mode, TimerQueueIface fsQ, TimerQueueIface segsQ) throws IOException, KawkabException {
		this.inumber = inumber;
		this.fileMode = mode;
		this.fsQ = fsQ;

		onPrimaryNode = Configuration.instance().thisNodeID == Commons.primaryWriterID(inumber); //Is this reader or writer on the primary node?

		int inodesBlockIdx = (int) (inumber / inodesPerBlock);
		BlockID id = new InodesBlockID(inodesBlockIdx);
		
		InodesBlock inb = (InodesBlock) cache.acquireBlock(id);
		inb.loadBlock();

		inodesBlock = inb;
		inode = inb.getInode(inumber);
		inode.prepare(fsQ, segsQ);
		if (mode == FileMode.APPEND) //Pre-fetch the last block for writes
			inode.loadLastBlock();

		rLog = new TimeLog(TimeLog.TimeLogUnit.NANOS, "R-"+inumber, 1);
		wLog = new TimeLog(TimeLog.TimeLogUnit.NANOS, "W-"+inumber, 1);
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
	public synchronized int read(byte[] buffer, long readOffsetInFile, int length) throws OutOfMemoryException,
			IOException, IllegalArgumentException, KawkabException, InvalidFileOffsetException {
		if (length > buffer.length || length < 0)
			throw new IllegalArgumentException("Read length is negative or greater than the given buffer size.");

		rLog.start();

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
				int blockIndex = (int) (inumber / inodesPerBlock);
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

		rLog.end();
		return bytesRead;
	}

	/**
	 * @param minTS
	 * @param maxTS
	 * @param recSize Size of a record that was appended to the file
	 * @return
	 * @throws KawkabException
	 * @throws IOException
	 */
	public synchronized List<ByteBuffer> readRecords(final long minTS, final long maxTS, final int recSize)
			throws OutOfMemoryException, KawkabException, IOException {
		rLog.start();
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
				int blockIndex = (int) (inumber / inodesPerBlock);
				BlockID id = new InodesBlockID(blockIndex);
				inb = (InodesBlock) cache.acquireBlock(id);
				inb.loadBlock();
				inode = inb.getInode(inumber);
			}

			rLog.end();
			return inode.readRecords(minTS, maxTS, recSize);
		} finally {
			if (!onPrimaryNode && inb != null) {
				cache.releaseBlock(inb.id());
			}
		}
	}

	public synchronized List<Record> readRecords(final long minTS, final long maxTS, final Record recFactory) throws OutOfMemoryException, KawkabException, IOException {
		rLog.start();
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
				int blockIndex = (int) (inumber / inodesPerBlock);
				BlockID id = new InodesBlockID(blockIndex);
				inb = (InodesBlock) cache.acquireBlock(id);
				inb.loadBlock();
				inode = inb.getInode(inumber);
			}

			rLog.end();
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
	 * @param dstBuf Buffer where the read record will be copied
	 * @param timestamp Exact match timestamp
	 * @param recSize Size of the record when the record as appended
	 *
	 * @return Whether the record is found and loaded
	 * @throws IOException
	 * @throws KawkabException
	 * @throws InterruptedException
	 */
	public synchronized boolean recordAt(final ByteBuffer dstBuf, final long timestamp, final int recSize) throws
			OutOfMemoryException, IOException, RecordNotFoundException, KawkabException {

		rLog.start();
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
				int blockIndex = (int) (inumber / inodesPerBlock);
				BlockID id = new InodesBlockID(blockIndex);
				inb = (InodesBlock) cache.acquireBlock(id);
				inb.loadBlock();
				inode = inb.getInode(inumber);
			}

			rLog.end();
			return inode.readAt(dstBuf, timestamp, recSize);
		} finally {
			if (!onPrimaryNode && inb != null) {
				cache.releaseBlock(inb.id());
			}
		}
	}
	
	/**
	 * Loads the dstRecord from the record number recordNum in the file.
	 *
	 * @param dstBuf Buffer where the read record will be copied
	 * @param recordNum the record number in the file, 1 being the first record. recNum should be greater than 0.
	 * @param recSize Size of the record when the record as appended
	 *
	 * @return Whether the record is found and loaded
	 *
	 * @throws IOException
	 * @throws KawkabException
	 * @throws RecordNotFoundException if the record does not exist in the file
	 * @throws InvalidFileOffsetException if the recordNum is less than 1
	 */
	public synchronized boolean recordNum(final ByteBuffer dstBuf, final long recordNum, final int recSize) throws
			OutOfMemoryException, IOException, KawkabException, RecordNotFoundException, InvalidFileOffsetException {
		rLog.start();

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
				int blockIndex = (int) (inumber / inodesPerBlock);
				BlockID id = new InodesBlockID(blockIndex);
				inb = (InodesBlock) cache.acquireBlock(id);
				inb.loadBlock();
				inode = inb.getInode(inumber);
			}

			rLog.end();
			return inode.readRecordN(dstBuf, recordNum, recSize);
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
	
	public synchronized int append(byte[] data, int offset, int length) throws OutOfMemoryException, MaxFileSizeExceededException,
									IOException, KawkabException, InterruptedException{
		wLog.start();

		if (fileMode != FileMode.APPEND || !onPrimaryNode) {
			throw new InvalidFileModeException();
		}

		if (inodesBlock == null) {
			throw new KawkabException("The file handle is closed. Open the file again to get the new handle.");
		}

		int appendedBytes = inode.appendBuffered(data, offset, length);

		if (inbAcquired == null || !fsQ.tryDisable(inbAcquired)) {
			inbAcquired = new TimerQueueItem<>(inodesBlock, this);
		}

		inbAcquired.getItem().markLocalDirty();
		fsQ.enableAndAdd(inbAcquired, clock.currentTime() + bufferTimeLimitMs);

		wLog.end();
		return appendedBytes;
	}
	
	// FIXME: Create a separate interface for binary and structured files. Binary append and record-based appends
	// are mutually exclusive. They should not be mixed in the same file.
	
	/**
	 * Append data at the end of the file and index with the given key
	 *
	 * @return Number of bytes appended
	 * dataIndex.timestamp() to refer to the data just written.
	 * @throws OutOfMemoryException
	 * @throws InvalidFileOffsetException
	 * @throws IOException
	 * @throws KawkabException
	 * @throws InterruptedException
	 */
	public synchronized int append(final ByteBuffer srcBuf, int recSize) throws OutOfMemoryException, MaxFileSizeExceededException,
			IOException, KawkabException, InterruptedException{
		if (fileMode != FileMode.APPEND || !onPrimaryNode) {
			throw new InvalidFileModeException();
		}

		if (inodesBlock == null) {
			throw new KawkabException("The file handle is closed. Open the file again to get the new handle.");
		}

		wLog.start();

		int appendedBytes = inode.appendRecords(srcBuf, recSize);

		if (inbAcquired == null || !fsQ.tryDisable(inbAcquired)) {
			inbAcquired = new TimerQueueItem<>(inodesBlock, this);
		}

		inbAcquired.getItem().markLocalDirty();
		fsQ.enableAndAdd(inbAcquired, clock.currentTime() + bufferTimeLimitMs);

		wLog.end();
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

	public synchronized long recordsInFile() throws KawkabException {
		if (onPrimaryNode) {
			if (inodesBlock == null) {
				throw new KawkabException("The file handle is closed. Open the file again to get the new handle.");
			}

			return inode.recordsInFile();
		}

		int inodesBlockIdx = (int)(inumber / inodesPerBlock);
		BlockID id = new InodesBlockID(inodesBlockIdx);

		InodesBlock block = null;
		try {
			block = (InodesBlock) cache.acquireBlock(id);
			block.loadBlock();
			return block.getInode(inumber).recordsInFile();
		} catch (IOException e) {
			e.printStackTrace();
			throw new KawkabException(e);
		} finally {
			if (block != null) {
				cache.releaseBlock(block.id());
			}
		}
	}
	
	public long inumber() { // For debugging only
		return inumber;
	}
	
	public FileMode mode() {
		return fileMode;
	}
	
	synchronized void close() throws KawkabException {
		if (inbAcquired != null && fsQ.tryDisable(inbAcquired)) {
			deferredWork(inbAcquired.getItem());
		}

		if (inodesBlock != null) {
			inode.cleanup(); //FIXME: This will cleanup for all the clients that have opened the file, which is wrong.
			cache.releaseBlock(inodesBlock.id());
		}

		inode = null;
		inodesBlock = null;

		if (rLog.sampled() > 0) {
			System.out.print("File "+inumber+" read stats: ");
			rLog.printStats();
		}

		if (wLog.sampled() > 0) {
			System.out.print("File "+inumber+" append stats: ");
			wLog.printStats();
		}
		
		//TODO: Update openFiles table.
	}

	@Override
	public void deferredWork(InodesBlock ib) {
		try {
			localStore.store(ib);
		} catch (KawkabException e) {
			e.printStackTrace();
		}
	}
}
