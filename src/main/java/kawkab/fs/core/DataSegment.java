package kawkab.fs.core;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.protobuf.ByteString;

import kawkab.fs.commons.Commons;
import kawkab.fs.commons.Constants;
import kawkab.fs.core.exceptions.FileNotExistException;
import kawkab.fs.core.exceptions.InsufficientResourcesException;
import kawkab.fs.core.exceptions.InvalidFileOffsetException;
import kawkab.fs.core.exceptions.KawkabException;

public final class DataSegment extends Block {
	private final static int recordSize = Constants.recordSize; //Temporarily set to 1 until we implement reading/writing records
	
	//private byte[] bytes;
	private ByteBuffer dataBuf;
	
	private DataSegmentID segmentID;
	
	//Keeps track of the offset and length of dirty bytes
	private int dirtyBytesStart;  // Offset of the dirty bytes that have not been synced to the local store yet
	private int dirtyBytesLength; // Number of the dirty bytes
	private boolean segmentIsFull;  // Sets to true only when the block becomes full in an append operation.
	private final Lock dirtyBytesLock; // To atomically update dirty bytes start and length
	private long lastGlobalFetchTimeMs; // Clock time in ms when the block was last loaded. This must be initialized 
										// to zero when the block is first created in memory.
	//private int bytesFilled; //Number of valid bytes in the segment, starting from the first byte
	
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
		//bytes = new byte[Constants.segmentSizeBytes];
		dirtyBytesStart = Constants.segmentSizeBytes; //This initial value is important for the correct functioning of appendOffsetInBlock()
		dirtyBytesLength = 0; //The variable is updated to the correct value in adjustDirtyOffsets()
		dirtyBytesLock = new ReentrantLock();
		lastGlobalFetchTimeMs = 0;
		
		//System.out.println(" Opened block: " + name());
	}
	
	/**
	 * @param data Data to be appended
	 * @param offset  Offset in data
	 * @param length  Number of bytes to append: data[offset] to data[offset+length-1] inclusive.
	 * @param offsetInFile  
	 * @return number of bytes appended starting from the offset
	 * @throws IOException 
	 */
	synchronized int append(byte[] data, int offset, int length, long offsetInFile) throws IOException {
		assert offset >= 0;
		assert offset < data.length;
		
		//int offsetInBlock = (int)(offsetInFile % Constants.segmentSizeBytes);
		int offsetInSegment = offsetInSegment(offsetInFile, recordSize);
		int capacity = Constants.segmentSizeBytes - offsetInSegment;
		int toAppend = length <= capacity ? length : capacity;
		
		//System.arraycopy(data, offset, bytes, offsetInSegment, toAppend);
		
		assert offsetInSegment == dataBuf.limit();
		
		dataBuf.position(offsetInSegment);
		dataBuf.limit(offsetInSegment + toAppend);
		dataBuf.put(data, offset, toAppend);
		
		adjustDirtyOffsets(offsetInSegment, toAppend);
		markDirty();
		
		//Mark block as full
		if (offsetInSegment+toAppend == Constants.segmentSizeBytes) {
			segmentIsFull = true;
		}
		
		//bytesFilled += toAppend;
		
		return toAppend;
	}
	
	/*synchronized int writeLong(long data, long offsetInFile) throws IOException{
		//int offsetInBlock = (int)(offsetInFile % Constants.segmentSizeBytes);
		int offsetInBlock = offsetInSegment(offsetInFile, recordSize);
		int capacity = Constants.segmentSizeBytes - offsetInBlock;
		int longSize = Long.BYTES;
		if (capacity < longSize)
			return 0;
		
		ByteBuffer buffer = ByteBuffer.wrap(bytes);
		buffer.putLong(offsetInBlock, data);

		adjustDirtyOffsets(offsetInBlock, longSize);
		markDirty();
		
		return longSize;
	}*/
	
	/*synchronized long readLong(long offsetInFile) throws InvalidFileOffsetException, IOException{
		//int blockOffset = (int)(fileOffsetBytes % Constants.dataBlockSizeBytes);
		
		//int offsetInBlock = (int)(offsetInFile % Constants.segmentSizeBytes);
		int offsetInBlock = offsetInSegment(offsetInFile, recordSize);
		int blockSize = Constants.segmentSizeBytes;
		
		if (offsetInBlock >= blockSize || 
				offsetInBlock+Long.BYTES > blockSize || offsetInBlock < 0)
			throw new InvalidFileOffsetException(
					String.format("File Offset %d is invalid. Data block size = %d bytes.", 
							offsetInBlock, blockSize));
		
		ByteBuffer buffer = ByteBuffer.wrap(bytes);
		return buffer.getLong(offsetInBlock);
	}*/
	
	/*synchronized int writeInt(int data, long offsetInFile) throws IOException{
		//int offsetInBlock = (int)(offsetInFile % Constants.segmentSizeBytes);
		int offsetInBlock = offsetInSegment(offsetInFile, recordSize);
		int capacity = Constants.segmentSizeBytes - offsetInBlock;
		int intSize = Integer.BYTES;
		if (capacity < intSize)
			return 0;
		
		//buffer.putInt(offsetInBlock, data);
		
		ByteBuffer buffer = ByteBuffer.wrap(bytes);
		buffer.putInt(offsetInBlock, data);
		adjustDirtyOffsets(offsetInBlock, intSize);
		markDirty();
		
		return intSize;
	}*/
	
	/*synchronized int readInt(long offsetInFile) throws InvalidFileOffsetException, IOException{
		//int blockOffset = (int)(fileOffsetBytes % Constants.dataBlockSizeBytes);
		
		int blockSize = Constants.segmentSizeBytes;
		//int offsetInBlock = (int)(offsetInFile % Constants.segmentSizeBytes);
		int offsetInBlock = offsetInSegment(offsetInFile, recordSize);
		if (offsetInBlock >= blockSize || 
				offsetInBlock+4 >= blockSize || offsetInBlock < 0) {
			throw new InvalidFileOffsetException(
					String.format("File Offset %d is invalid. Data block size = %d bytes.", 
							offsetInBlock, blockSize));
		}
		
		//file.seek(offsetInBlock);
		//return file.readInt();
		
		ByteBuffer buffer = ByteBuffer.wrap(bytes);
		return buffer.getInt(offsetInBlock);
	}*/
	
	/**
	 * @param dstBuffer output buffer
	 * @param dstBufferOffset offset in the buffer to where data will be copied
	 * @param length length of data to be read
	 * @param offsetInBlock offset in this block from where to read data, offset starts from zero.
	 * @return number of bytes read starting from the offsetInFile
	 * @throws IOException 
	 * @throws IncorrectOffsetException 
	 */
	synchronized int read(byte[] dstBuffer, int dstBufferOffset, int length, long offsetInFile) 
			throws InvalidFileOffsetException, IOException{
		int blockSize = Constants.segmentSizeBytes;
		//int offsetInBlock = (int)(offsetInFile % Constants.segmentSizeBytes);
		int offsetInBlock = offsetInSegment(offsetInFile, recordSize);
		if (offsetInBlock >= blockSize || offsetInBlock < 0) {
			throw new InvalidFileOffsetException(
					String.format("Given file offset %d is outside of the block. Block size = %d bytes.",
					offsetInBlock, blockSize));
		}
		
		int readSize = offsetInBlock+length <= blockSize ? length : blockSize-offsetInBlock;
		//System.arraycopy(bytes, offsetInBlock, dstBuffer, dstBufferOffset, readSize);
		
		dataBuf.rewind();
		dataBuf.position(offsetInBlock);
		dataBuf.get(dstBuffer, dstBufferOffset, readSize);
		
		return readSize;
	}
	
	@Override
	public synchronized boolean shouldStoreGlobally() {
		// Store in the global store only if this is the last segment of the block and this segment is full
		if (segmentID.segmentInBlock()+1 == Constants.segmentsPerBlock // If it is the last segment in the block 
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
	public synchronized void loadFrom(ByteBuffer srcBuffer) throws IOException {
		/*if (srcBuffer.remaining() < Constants.segmentSizeBytes) { // If we use a ByteBuffer to store data bytes, we send only valid bytes 
																// to a remote node. Therefore, this check is not required if we use 
																// ByteBuffer. Otherwise, if we use a byte array, we need to check this as 
																// we send all the bytes regardless of their validity.
			throw new InsufficientResourcesException(String.format("Not enough bytes left in the buffer: "
					+ "Have %d, needed %d.",srcBuffer.remaining(), Constants.segmentSizeBytes));
		}*/
		
		//bytes = new byte[Constants.segmentSizeBytes];
		//buffer.get(bytes);
		
		srcBuffer.rewind();
		
		System.out.println("[DS] Bytes to load from the buffer = " + srcBuffer.remaining());
		
		dataBuf = ByteBuffer.allocateDirect(Constants.segmentSizeBytes);
		dataBuf.limit(srcBuffer.remaining());
		dataBuf.put(srcBuffer);
		dataBuf.rewind();
		
		//FIXME: What if the number of bytes read is less than the block size?
		//if (bytesRead < bytes.length)
		//	throw new InsufficientResourcesException(String.format("Full block is not loaded. Loaded "
		//			+ "%d bytes out of %d.",bytesRead,bytes.length));
	}
	
	@Override
	public synchronized void loadFrom(ReadableByteChannel channel) throws IOException {
		/*bytes = new byte[Constants.segmentSizeBytes];
		ByteBuffer buffer = ByteBuffer.wrap(bytes);
		Commons.readFrom(channel, buffer);*/ 
		
		dataBuf = ByteBuffer.allocateDirect(Constants.segmentSizeBytes);
		int loaded = Commons.readFrom(channel, dataBuf);
		
		assert loaded >= 0;
		
		dataBuf.limit(loaded);
		dataBuf.rewind();
		
		System.out.printf("[DS] Loaded bytes from channel: %d, bufLimit=%d\n", loaded, dataBuf.limit());
		
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
		
		long now = System.currentTimeMillis();
		
		if (lastGlobalFetchTimeMs < now - Constants.globalFetchExpiryTimeoutMs) { // If the last fetch from the global store has expired
				
			now = System.currentTimeMillis();
			
			if (lastGlobalFetchTimeMs < now - Constants.globalFetchExpiryTimeoutMs) { // If the last fetch from the global store has expired
				try {
					System.out.println("[B] Load from the global: " + id());
					
					loadFromGlobal(); // First try loading data from the global store
					lastGlobalFetchTimeMs = Long.MAX_VALUE; // Never expire data fetched from the global store. 
															// InodeBlocks are updated to the global store (not treating as an append only system).
					return;
					//TODO: If this block cannot be further modified, never expire the loaded data. For example, if it was the last segment of the block.
				} catch (FileNotExistException e) { //If the block is not in the global store yet
					System.out.println("[B] Not found in the global: " + id());
					lastGlobalFetchTimeMs = 0; // Failed to fetch from the global store
				}
			
				System.out.println("[B] Primary fetch expired or not found from the global: " + id());
				
				try {
					System.out.println("[B] Loading from the primary: " + id());
					loadBlockFromPrimary(); // Fetch from the primary node
					//lastPrimaryFetchTimeMs = now;
					if (lastGlobalFetchTimeMs == 0) // Set to now if the global fetch has failed
						lastGlobalFetchTimeMs = now;
				} catch (FileNotExistException ke) { // If the file is not on the primary node, check again from the global store
					// Check again from the global store because the primary may have deleted the 
					// block after copying to the global store
					System.out.println("[B] Not found on the primary, trying again from the global: " + id());
					loadFromGlobal(); 
					lastGlobalFetchTimeMs = now;
					//lastPrimaryFetchTimeMs = 0;
				} catch (IOException ioe) {
					System.out.println("[B] Not found in the global and the primary: " + id());
					throw new KawkabException(ioe);
				}
			}
		}
	}
	
	/*@Override
	public int fromInputStream(InputStream in) throws IOException {
		lock();
		bytes = new byte[Constants.segmentSizeBytes];
		int read = 0;
		int remaining = bytes.length;
		int ret = 0;
		try {
			while(remaining > 0 && ret >= 0) {
				ret = in.read(bytes, read, remaining);
				remaining -= ret;
				read += ret;
			}
		} finally {
			unlock();
		}
		
		if (read != bytes.length)
			throw new IOException("Unable to load Ibmap completely from the inputstream: " + name());
		
		return read;
	}*/
	
	@Override
	public synchronized int storeTo(WritableByteChannel channel) throws IOException {
		int bytesWritten = 0;
		
		dirtyBytesLock.lock();
			int dirtyBytesOffset = dirtyBytesStart;
			int dirtyBytesSize   = dirtyBytesLength;
	
			if(dirtyBytesSize == 0){
			dirtyBytesLock.unlock();
				return 0;
			}
		
			dirtyBytesStart += dirtyBytesSize;
			dirtyBytesLength -= dirtyBytesSize;		
		
		dirtyBytesLock.unlock();
		
		
		try {
			/*ByteBuffer buffer = ByteBuffer.wrap(bytes, dirtyBytesOffset, dirtyBytesSize);
			int size = buffer.remaining();
			bytesWritten = Commons.writeTo(channel, buffer);*/
			
			
			dataBuf.position(dirtyBytesOffset);
			//dataBuf.limit(dirtyBytesOffset+dirtyBytesSize);
			int size = dataBuf.remaining();
			while(bytesWritten < size) {
	    		bytesWritten += channel.write(dataBuf);
	    	}
			dataBuf.rewind();
			
			if (bytesWritten < size) {
				throw new InsufficientResourcesException(String.format("Full block is not stored. Stored "
						+ "%d bytes out of %d.",bytesWritten, size));
			}
		} catch (IOException e) { 
			dirtyBytesLock.lock();

			dirtyBytesStart -= dirtyBytesSize;   // We can reverse the changes because always the same worker thread in the LocalProcessor
			dirtyBytesLength += dirtyBytesSize;  // locally stores the segments of the same file

			dirtyBytesLock.unlock();

			throw e;
		}
		
		System.out.printf("[DS] Bytes written in channel = %d, dirtyOffset=%d, dirtyLenght=%d\n", bytesWritten, dirtyBytesOffset, dirtyBytesSize);
		
		return bytesWritten;
	}

	@Override
	public synchronized int storeFullTo(WritableByteChannel channel) throws IOException {
		int bytesWritten = 0;
		//int size = bytes.length;
		//ByteBuffer buffer = ByteBuffer.wrap(bytes);
		//bytesWritten = Commons.writeTo(channel, buffer);
		
		dataBuf.rewind();
		int size = dataBuf.remaining();
		bytesWritten = Commons.writeTo(channel, dataBuf);
		
		if (bytesWritten < size) {
			throw new InsufficientResourcesException(String.format("Full block is not stored. Stored "
					+ "%d bytes out of %d.",bytesWritten, size));
		}
		
		System.out.println("[DS] Bytes written in channel: " + bytesWritten);
		
		return bytesWritten;
	}
	
	@Override
	public synchronized ByteString byteString() {
		//return ByteString.copyFrom(bytes); //TODO: Send only usable bytes instead of sending the complete segment
		/*dataBuf.rewind();
		ByteString str = ByteString.copyFrom(new byte[] {(byte)(segmentIsFull?1:0)});
		return str.concat(ByteString.copyFrom(dataBuf));*/
		dataBuf.rewind();
		return ByteString.copyFrom(dataBuf);
	}
	
	/**
	 * Returns the append offset with respect to the start of the block instead of the segment
	 */
	@Override
	public int appendOffsetInBlock() {
		dirtyBytesLock.lock();
		int offsetInSegment = dirtyBytesLength > 0 ? dirtyBytesStart : 0;
		dirtyBytesLock.unlock();
		
		//Offset in block is equal to the start offset of the current segment + the start of the dirty bytes
		int offset = segmentID.segmentInBlock() * Constants.segmentSizeBytes + offsetInSegment;
		
		assert offset <= Constants.dataBlockSizeBytes;
		
		return offset;
	}
	
	/**
	 * Updates the dirty bytes start and length
	 * 
	 * @param currentAppendPosition
	 * @param length
	 */
	private void adjustDirtyOffsets(int currentAppendPosition, int length) {
		dirtyBytesLock.lock();
		
		if(currentAppendPosition < dirtyBytesStart) { //This will be true when the segment is loaded and no append is done yet
			dirtyBytesStart = currentAppendPosition;
		}
		
		if (dirtyBytesStart + dirtyBytesLength < currentAppendPosition + length) //If the previous dirty bytes have not been saved locally
			dirtyBytesLength += (currentAppendPosition + length) - (dirtyBytesStart + dirtyBytesLength);
		
		dirtyBytesLock.unlock();
	}
	
	@Override
	public int memorySizeBytes() {
		return Constants.segmentSizeBytes + 8; //FIXME: Get the exact number
	}
	
	@Override
	public int sizeWhenSerialized() {
		return Constants.segmentSizeBytes;
	}
	
	
	/*@Override
	void fromBuffer(ByteBuffer buffer) throws InsufficientResourcesException {
		//We don't have to anything here because the loadFromDisk function implemented in this class does not call fromBuffer method. 
	}

	@Override
	void toBuffer(ByteBuffer buffer) throws InsufficientResourcesException {
		//We don't have to anything here because the storeToDisk function implemented in this class does not call toBuffer method.
	}
	
	@Override
	public void load() throws IOException {
		//System.out.println("Opening block: " + id);
		Path path = new File(localPath()).toPath();
		//buffer = IoUtil.mapExistingFile(location, id.key, 0, Constants.dataBlockSizeBytes);
		Set<OpenOption> options = new HashSet<OpenOption>();
	    options.add(StandardOpenOption.WRITE);
	    options.add(StandardOpenOption.READ);
		channel = Files.newByteChannel(path, options);
	}*/
	
	
	/**
	 * This is the last function called before releasing memory from cache. Therefore, we should also perform
	 * cleanup operations here.
	 */
	/*@Override
	public void store() throws IOException{
		//System.out.println("Closing block: " + id);
		//IoUtil.unmap(buffer);
		//System.out.println("\tClosed block: " + name());
		cleanup();
	}*/
	
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
		return Constants.segmentSizeBytes / recordSize;
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
		return segmentInFile * Constants.segmentSizeBytes / Constants.dataBlockSizeBytes;
	}
	
	/**
	 * Returns the segment number in the block that corresponds to the given segment number in the file
	 */
	public static int segmentInBlock(long segmentInFile) {
		// segmentInBlock := segmentInFile % segmentsPerBlock
		return (int)(segmentInFile % Constants.segmentsPerBlock);
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
	
	/**
	 * Converts the offset in the file to the offset in the block
	 */
	private static int offsetInBlock(long offsetInFile, int recordSize) {
		// offsetInBlock = segmentInBlock + recordInSegment + offsetInRecord
		return (int)((segmentInBlock(segmentInFile(offsetInFile, recordSize)) + recordInSegment(offsetInFile, recordSize) + (offsetInFile % recordSize)));
	}
}
