package kawkab.fs.core;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.protobuf.ByteString;

import kawkab.fs.commons.Commons;
import kawkab.fs.commons.Configuration;
import kawkab.fs.core.exceptions.FileNotExistException;
import kawkab.fs.core.exceptions.InsufficientResourcesException;
import kawkab.fs.core.exceptions.InvalidFileOffsetException;
import kawkab.fs.core.exceptions.KawkabException;

public final class DataSegment extends Block {
	private final static Configuration conf = Configuration.instance();
	private final static int recordSize = conf.recordSize; //Temporarily set to 1 until we implement reading/writing records
	
	private ByteBuffer dataBuf;
	private DataSegmentID segmentID;
	private volatile boolean segmentIsFull;  // Sets to true only when the block becomes full in an append operation.
	private long lastFetchTimeMs; // Clock time in ms when the block was last loaded. This must be initialized 
										// to zero when the block is first created in memory.
	
	private AtomicInteger writePos; // Keeps track of the next byte offset where data is appended
	
	private int dirtyOffset;	// The offset where the dirty bytes start, the bytes that are not persisted yet. 
								// dirtyOffset doesn't need to be thread-safe because only the localStore thread reads and updates its value.
								// The value is initially set when the block is loaded, at which point the block cannot be accessed by the localStore.
	
	/**
	 * The constructor should not create a new file in the local storage. This constructor
	 * does not reads data from the underlying file. Instead, use loadFrom and storeTo 
	 * functions for that purpose.
	 * 
	 * @param uuid
	 */
	DataSegment(DataSegmentID segmentID) {
		super(segmentID);
		this.segmentID = segmentID;
		lastFetchTimeMs = 0;
		dirtyOffset = 0;
		writePos = new AtomicInteger(0);
		
		dataBuf = ByteBuffer.allocateDirect(conf.segmentSizeBytes);
	}
	
	void reInit(DataSegmentID segmentID) {
		reset(segmentID);
		this.segmentID = segmentID;
		lastFetchTimeMs = 0;
		dataBuf.clear();
		dirtyOffset = 0;
		writePos.set(0);
	}
	
	void initForAppend(long offsetInFile) {
		if (writePos.get() > 0) //This means the segment is already initialized and contains the valid bytes
			return;
		
		int initialAppendPos = offsetInSegment(offsetInFile, recordSize);
		writePos.set(initialAppendPos);
	}
	
	boolean isFull() {
		return segmentIsFull;
	}
	
	/**
	 * @param data Data to be appended
	 * @param offset  Offset in data
	 * @param length  Number of bytes to append: data[offset] to data[offset+length-1] inclusive.
	 * @param offsetInFile  
	 * @return number of bytes appended starting from the offset
	 * @throws IOException 
	 */
	int append(byte[] data, int offset, int length, long offsetInFile) throws IOException {
		assert offset >= 0;
		assert offset < data.length;
		
		int offsetInSegment = offsetInSegment(offsetInFile, recordSize);
		int capacity = conf.segmentSizeBytes - offsetInSegment;
		int toAppend = length <= capacity ? length : capacity;
		
		
		int writePntr = this.writePos.get();
		
		if (offsetInSegment != writePntr)
			System.out.println(id + " - " + offsetInSegment+" != "+writePntr);
		
		assert offsetInSegment == writePntr;

		dataBuf.put(data, offset, toAppend);
		
		writePos.addAndGet(toAppend);
		
		//adjustDirtyOffsets(offsetInSegment, toAppend);
		//Mark block as full
		if (offsetInSegment+toAppend == conf.segmentSizeBytes) {
			segmentIsFull = true;
		}
		
		markLocalDirty();
		
		//bytesFilled += toAppend;
		
		return toAppend;
	}
	
	/**
	 * @param dstBuffer output buffer
	 * @param dstBufferOffset offset in the buffer to where data will be copied
	 * @param length length of data to be read
	 * @param offsetInBlock offset in this block from where to read data, offset starts from zero.
	 * @return number of bytes read starting from the offsetInFile
	 * @throws IOException 
	 * @throws IncorrectOffsetException 
	 */
	int read(byte[] dstBuffer, int dstBufferOffset, int length, long offsetInFile) 
			throws InvalidFileOffsetException, IOException{
		int blockSize = conf.segmentSizeBytes;
		int offsetInBlock = offsetInSegment(offsetInFile, recordSize);
		if (offsetInBlock >= blockSize || offsetInBlock < 0) {
			throw new InvalidFileOffsetException(
					String.format("Given file offset %d is outside of the block. Block size = %d bytes.",
					offsetInBlock, blockSize));
		}
		
		int readSize = offsetInBlock+length <= blockSize ? length : blockSize-offsetInBlock;
		
		ByteBuffer buf = dataBuf.asReadOnlyBuffer(); //FIXME: Ensure that this is thread-safe in the presence of a concurrent writer!!!
		
		buf.rewind(); // We are not worried about the limit or writePos because the reader cannot read beyond the file size
		buf.position(offsetInBlock);
		buf.get(dstBuffer, dstBufferOffset, readSize);
		
		return readSize;
	}
	
	@Override
	public boolean shouldStoreGlobally() {
		// Store in the global store only if this is the last segment of the block and this segment is full
		if (segmentID.segmentInBlock()+1 == conf.segmentsPerBlock // If it is the last segment in the block 
				&& segmentIsFull) { // and the segment is full
			return true;
		}
		
		return false;
	}
	
	@Override
	public boolean evictLocallyOnMemoryEviction() {
		return shouldStoreGlobally();
	}
	
	@Override
	public synchronized void loadFromFile() throws IOException {
		try (
				RandomAccessFile file = new RandomAccessFile(id.localPath(), "r");
				SeekableByteChannel channel = file.getChannel()
			) {
			
			channel.position(segmentID.segmentInBlock() * conf.segmentSizeBytes);
			loadFrom(channel);
		}
	}
	
	@Override
	public synchronized void loadFrom(ReadableByteChannel channel) throws IOException {
		if (writePos.get() > 0)
			assert false; // FIXME: This means the segment is partially loaded by the writer without calling this function. We should load data only up to writePos. 
		
		int loaded = Commons.readFrom(channel, dataBuf);
		writePos.set(loaded);
		dirtyOffset = loaded;
		
		markLoaded();
		
		//System.out.printf("[DS] Loaded bytes from channel: %d, bufLimit=%d\n", loaded, dataBuf.limit());
		
		//FIXME: What if the number of bytes read is less than the segment size?
	}
	
	@Override
	public synchronized void loadFrom(ByteBuffer srcBuffer) throws IOException {
		/*if (srcBuffer.remaining() < Constants.segmentSizeBytes) { // If we use a ByteBuffer to store data bytes, we send only valid bytes 
																// to a remote node. Therefore, this check is not required if we use 
																// ByteBuffer. Otherwise, if we use a byte array, we need to check this as 
																// we send all the bytes regardless of their validity.
			throw new InsufficientResourcesException(String.format("Not enough bytes left in the buffer: "
					+ "Have %d, needed %d.",srcBuffer.remaining(), Constants.segmentSizeBytes));
		}*/
		
		srcBuffer.rewind();
		
		System.out.println("[DS] Bytes to load from the buffer = " + srcBuffer.remaining());
		
		dataBuf.put(srcBuffer);
		
		dirtyOffset = dataBuf.position();
		writePos.set(dataBuf.position());
		
		markLoaded();
		
		//FIXME: What if the number of bytes read is less than the block size?
		//if (bytesRead < bytes.length)
		//	throw new InsufficientResourcesException(String.format("Full block is not loaded. Loaded "
		//			+ "%d bytes out of %d.",bytesRead,bytes.length));
	}
	
	@Override
	protected synchronized void loadBlockFromPrimary() throws FileNotExistException, KawkabException, IOException {
		primaryNodeService.getSegment(segmentID, this);
	}
	
	@Override
	protected synchronized void loadBlockOnNonPrimary() throws FileNotExistException, KawkabException, IOException {
		/* If never fetched or the last global-fetch has timed out, fetch from the global store.
		 * Otherwise, if the last primary-fetch has timed out, fetch from the primary node.
		 * Otherwise, don't fetch, data is still fresh. 
		 */
		
		/**
		 * FIXME: The current implementation of this function is not ideal. Currently, an unfinished segment is reloaded
		 * from a remote source after a timeout. However, a segment should only be reloaded from a remote node if the
		 * read size exceeds the valid bytes in the loaded segment. In the current implementation, an unfinished
		 * data segment may get loaded from the remote node unnecessarily. 
		 */
		
		if (writePos.get() == conf.segmentSizeBytes) { // Never load an already loaded block if the segment is full.
			//System.out.println("[DS] Segment is full. Not loading the segment again.");
			return;
		}
		
		long now = System.currentTimeMillis();
		
		if (lastFetchTimeMs < now - conf.dataSegmentFetchExpiryTimeoutMs) { // If the last data-fetch-time exceeds the time limit
				
			now = System.currentTimeMillis();
			
			if (lastFetchTimeMs < now - conf.dataSegmentFetchExpiryTimeoutMs) { // If the last fetch from the global store has expired
				try {
					//System.out.println("[DS] Load from the global: " + id());
					
					loadFromGlobal(); // First try loading data from the global store
					lastFetchTimeMs = Long.MAX_VALUE; // Never expire data fetched from the global store.
					return;
					
					//TODO: If this block cannot be further modified, never expire the loaded data. For example, if it was the last segment of the block.
				} catch (FileNotExistException e) { //If the block is not in the global store yet
					System.out.println("[B] Not found in the global: " + id());
					lastFetchTimeMs = 0; // Failed to fetch from the global store
				}
			
				System.out.println("[B] Primary fetch expired or not found from the global: " + id());
				
				try {
					System.out.println("[B] Loading from the primary: " + id());
					loadBlockFromPrimary(); // Fetch data from the primary node
					//lastPrimaryFetchTimeMs = now;
					if (lastFetchTimeMs == 0) // Set to now if the global fetch has failed
						lastFetchTimeMs = now;
				} catch (FileNotExistException ke) { // If the file is not on the primary node, check again from the global store
					// Check again from the global store because the primary may have deleted the 
					// block after copying to the global store
					System.out.println("[B] Not found on the primary, trying again from the global: " + id());
					loadFromGlobal(); 
					lastFetchTimeMs = now;
					//lastPrimaryFetchTimeMs = 0;
				} catch (IOException ioe) {
					System.out.println("[B] Not found in the global and the primary: " + id());
					throw new KawkabException(ioe);
				}
			}
		}
	}
	
	@Override
	public int storeToFile() throws IOException {
		try (
				RandomAccessFile rwFile = new RandomAccessFile(id.localPath(), "rw");
				SeekableByteChannel channel = rwFile.getChannel()
			) {
			channel.position(appendOffsetInBlock());
			//System.out.println("Store: "+block.id() + ": " + channel.position());
			return storeTo(channel);
		}
	}
	
	@Override
	public int storeTo(WritableByteChannel channel) throws IOException {
		int limit = writePos.get();
		
		ByteBuffer buf = dataBuf.asReadOnlyBuffer(); //FIXME: Ensure that this is thread-safe in the presence of a concurrent writer!!!
			
		buf.position(dirtyOffset);
		buf.limit(limit);
		
		int size = buf.remaining();
		int bytesWritten = 0;
		try {
			while(bytesWritten < size) {
	    		bytesWritten += channel.write(buf);
	    	}
		} catch (IOException e) {
			throw e;
		}
		
		if (bytesWritten < size) {
			//FIXME: What should we do in this situation???
			throw new InsufficientResourcesException(String.format("Full block is not stored. Stored "
					+ "%d bytes out of %d.",bytesWritten, size));
		}
		
		dirtyOffset += bytesWritten;
		
		//System.out.printf("[DS] Bytes written in channel = %d, dirtyOffset=%d, dirtyLenght=%d\n", bytesWritten, dirtyBytesOffset, dirtyBytesSize);
		
		return bytesWritten;
	}

	
	@Override
	public ByteString byteString() {
		int limit = writePos.get();
		
		ByteBuffer buf = null;
		//synchronized(dataBuf) {
			buf = dataBuf.asReadOnlyBuffer();
		//}
		
		buf.rewind();
		return ByteString.copyFrom(buf, limit);
	}
	
	/**
	 * Returns the append offset with respect to the start of the block instead of the segment
	 */
	@Override
	public int appendOffsetInBlock() {
		//dirtyBytesLock.lock();
		//int offsetInSegment = dirtyBytesLength > 0 ? dirtyBytesStart : 0;
		//dirtyBytesLock.unlock();
		
		//Offset in block is equal to the start offset of the current segment + the start of the dirty bytes
		int offset = segmentID.segmentInBlock() * conf.segmentSizeBytes + dirtyOffset;
		
		assert offset <= conf.dataBlockSizeBytes;
		
		return offset;
	}
	
	@Override
	public int memorySizeBytes() {
		return conf.segmentSizeBytes + 8; //FIXME: Get the exact number
	}
	
	@Override
	public int sizeWhenSerialized() {
		return conf.segmentSizeBytes;
	}
	
	@Override
	public synchronized void cleanup() throws IOException {
		//bytes = null;
		dataBuf = null;
	}

	@Override
	public String toString(){
		return id().toString();
	}
	
	/**
	 * Number of records of the given size that can fit in a segment
	 * @param recordSize
	 * @return
	 */
	public static int recordsPerSegment(int recordSize) {
		return conf.segmentSizeBytes / recordSize;
	}
	
	/**
	 * Is the given segment the last segment in the block
	 */
	public static boolean isLastSegment(long segmentInFile, long fileSize, int recordSize) {
		return segmentInFile == segmentInFile(fileSize, recordSize);
	}
	
	/**
	 * Returns the segment number in the file that contains the given offset in the file
	 */
	public static long segmentInFile(long offsetInFile, int recordSize) {
		// segmentInFile := recordInFile / recordsPerSegment
		return (offsetInFile/recordSize) / recordsPerSegment(recordSize); 
	}
	
	/**
	 * Returns the block number in the file that contains the given segment number in the file
	 */
	public static long blockInFile(long segmentInFile) {
		// blockInFile := segmentInFile x segmentSize / blockSize
		return segmentInFile * conf.segmentSizeBytes / conf.dataBlockSizeBytes;
	}
	
	/**
	 * Returns the segment number in the block that corresponds to the given segment number in the file
	 */
	public static int segmentInBlock(long segmentInFile) {
		// segmentInBlock := segmentInFile % segmentsPerBlock
		return (int)(segmentInFile % conf.segmentsPerBlock);
	}
	
	/**
	 * Returns the record number in its segment that contains the given offset in the file
	 */
	public static int recordInSegment(long offsetInFile, int recordSize) {
		return (int)((offsetInFile/recordSize) % recordsPerSegment(recordSize));
	}
	
	/**
	 * Converts the offset in file to the offset in the segemnt
	 */
	public static int offsetInSegment(long offsetInFile, int recordSize) {
		// offsetInSegment = recordInSegment + offsetInRecord
		return (int)(recordInSegment(offsetInFile, recordSize) + (offsetInFile % recordSize));
	}
}
