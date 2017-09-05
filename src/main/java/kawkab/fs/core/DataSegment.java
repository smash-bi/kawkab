package kawkab.fs.core;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import kawkab.fs.commons.Commons;
import kawkab.fs.commons.Constants;
import kawkab.fs.core.exceptions.InsufficientResourcesException;
import kawkab.fs.core.exceptions.InvalidFileOffsetException;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class DataSegment extends Block {
	//private MappedByteBuffer buffer;
	//private SeekableByteChannel channel;
	private byte[] bytes;
	
	//Keeps track of the offset and length of dirty bytes
	private int dirtyBytesStart;
	private int dirtyBytesLength;
	
	private final Lock dirtyBytesLock;
	
	/**
	 * The constructor should not create a new file in the underlying filesystem. This constructor
	 * does not reads data from the underlying file. Instead, use loadFrom and storeTo 
	 * functions for that purpose.
	 * 
	 * This constructor should not create the bytes array. It is created in loadFrom function.
	 * @param uuid
	 */
	DataSegment(DataSegmentID uuid) {
		super(uuid);
		bytes = new byte[0];
		dirtyBytesStart = Constants.segmentSizeBytes; //The variable is updated to correct value in adjustDirtyOffsets()
		dirtyBytesLength = 0; //The variable is updated to correct value in adjustDirtyOffsets()
		dirtyBytesLock = new ReentrantLock();
		//System.out.println(" Opened block: " + name());
	}
	
	/**
	 * @param data Data to be appended
	 * @param offset  Offset in data
	 * @param length  Number of bytes to append: data[offset] to data[offset+length-1] inclusive.
	 * @param offsetInFile  
	 * @return number of bytes appended
	 * @throws IOException 
	 */
	synchronized int append(byte[] data, int offset, int length, long offsetInFile) throws IOException {
		assert offset >= 0;
		assert offset < data.length;
		
		int offsetInBlock = (int)(offsetInFile % Constants.segmentSizeBytes);
		int capacity = Constants.segmentSizeBytes - offsetInBlock;
		
		int toAppend = length <= capacity ? length : capacity;
		
		//long t = System.nanoTime();
		System.arraycopy(data, offset, bytes, offsetInBlock, toAppend);
		//long elapsed = (System.nanoTime() - t)/1000;
		//System.out.println("elaped: " + elapsed);
		
		//buffer.position(offsetInBlock);
		//buffer.put(data, offset, toAppend);
		

		/*ByteBuffer buffer = ByteBuffer.wrap(data, offset, toAppend);
		channel.position(offsetInBlock);
		int written = channel.write(buffer);*/
		
		adjustDirtyOffsets(offsetInBlock, toAppend);
		markDirty();
		
		//System.out.println("  Append: " + name() + " - " + toAppend);
		
		return toAppend;
	}
	
	synchronized int writeLong(long data, long offsetInFile) throws IOException{
		int offsetInBlock = (int)(offsetInFile % Constants.segmentSizeBytes);
		int capacity = Constants.segmentSizeBytes - offsetInBlock;
		int longSize = Long.BYTES;
		if (capacity < longSize)
			return 0;
		
		ByteBuffer buffer = ByteBuffer.wrap(bytes);
		buffer.putLong(offsetInBlock, data);

		adjustDirtyOffsets(offsetInBlock, longSize);
		markDirty();
		
		return longSize;
	}
	
	synchronized long readLong(long offsetInFile) throws InvalidFileOffsetException, IOException{
		//int blockOffset = (int)(fileOffsetBytes % Constants.dataBlockSizeBytes);
		
		int offsetInBlock = (int)(offsetInFile % Constants.segmentSizeBytes);
		int blockSize = Constants.segmentSizeBytes;
		
		if (offsetInBlock >= blockSize || 
				offsetInBlock+Long.BYTES > blockSize || offsetInBlock < 0)
			throw new InvalidFileOffsetException(
					String.format("File Offset %d is invalid. Data block size = %d bytes.", 
							offsetInBlock, blockSize));
		
		ByteBuffer buffer = ByteBuffer.wrap(bytes);
		return buffer.getLong(offsetInBlock);
	}
	
	synchronized int writeInt(int data, long offsetInFile) throws IOException{
		int offsetInBlock = (int)(offsetInFile % Constants.segmentSizeBytes);
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
	}
	
	synchronized int readInt(long offsetInFile) throws InvalidFileOffsetException, IOException{
		//int blockOffset = (int)(fileOffsetBytes % Constants.dataBlockSizeBytes);
		
		int blockSize = Constants.segmentSizeBytes;
		int offsetInBlock = (int)(offsetInFile % Constants.segmentSizeBytes);
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
	}
	
	/**
	 * @param dstBuffer output buffer
	 * @param dstBufferOffset offset in the buffer to where data will be copied
	 * @param length length of data to be read
	 * @param offsetInBlock offset in this block from where to read data, offset starts from zero.
	 * @return number of bytes read
	 * @throws IOException 
	 * @throws IncorrectOffsetException 
	 */
	synchronized int read(byte[] dstBuffer, int dstBufferOffset, int length, long offsetInFile) 
			throws InvalidFileOffsetException, IOException{
		int blockSize = Constants.segmentSizeBytes;
		int offsetInBlock = (int)(offsetInFile % Constants.segmentSizeBytes);
		if (offsetInBlock >= blockSize || offsetInBlock < 0) {
			throw new InvalidFileOffsetException(
					String.format("Given file offset %d is outside of the block. Block size = %d bytes.",
					offsetInBlock, blockSize));
		}
		
		int readSize = offsetInBlock+length <= blockSize ? length : blockSize-offsetInBlock;
		System.arraycopy(bytes, offsetInBlock, dstBuffer, dstBufferOffset, readSize);
		return readSize;
	}
	
	BlockID uuid(){
		return id;
	}
	
	@Override
	public void loadFrom(ReadableByteChannel channel) throws IOException {
		lock();
		try {
			bytes = new byte[Constants.segmentSizeBytes];
			ByteBuffer buffer = ByteBuffer.wrap(bytes);
			
			int bytesRead = Commons.readFrom(channel, buffer); 
			
			//FIXME: What if the number of bytes read is less than the block size?
			//if (bytesRead < bytes.length)
			//	throw new InsufficientResourcesException(String.format("Full block is not loaded. Loaded "
			//			+ "%d bytes out of %d.",bytesRead,bytes.length));
		} finally {
			unlock();
		}
	}
	
	@Override
	public void storeTo(WritableByteChannel channel) throws IOException {
		dirtyBytesLock.lock();
			int dirtyBytesOffset = dirtyBytesStart;
			int dirtyBytesSize   = dirtyBytesLength;
	
			if(dirtyBytesSize == 0){
				dirtyBytesLock.unlock();
				return;
			}
		
			dirtyBytesStart += dirtyBytesSize;
			dirtyBytesLength -= dirtyBytesSize;		
		
		dirtyBytesLock.unlock();
		
		lock();
		try {
			ByteBuffer buffer = ByteBuffer.wrap(bytes, dirtyBytesOffset, dirtyBytesSize);
			int remaining = buffer.remaining();
			int bytesWritten = Commons.writeTo(channel, buffer);
			
			if (bytesWritten < remaining) {
				throw new InsufficientResourcesException(String.format("Full block is not stored. Stored "
						+ "%d bytes out of %d.",bytesWritten, remaining));
			}
		} catch (IOException e) { 
			dirtyBytesLock.lock();

			dirtyBytesStart -= dirtyBytesSize;   // We can reverse the changes because always the same worker thread in the LocalProcessor
			dirtyBytesLength += dirtyBytesSize;  // locally stores the segments of the same file

			dirtyBytesLock.unlock();

			throw e;
		}
		finally {
			unlock();
		}
	}

	@Override
	public void storeFullTo(WritableByteChannel channel) throws IOException {
		lock();
		try {
			ByteBuffer buffer = ByteBuffer.wrap(bytes);
			int bytesWritten = Commons.writeTo(channel, buffer);
			
			if (bytesWritten < bytes.length) {
				throw new InsufficientResourcesException(String.format("Full block is not stored. Stored "
						+ "%d bytes out of %d.",bytesWritten, bytes.length));
			}
		} finally {
			unlock();
		}
	}
	
	@Override
	public int appendOffsetInBlock() {
		dirtyBytesLock.lock();
		int offsetInSegment = dirtyBytesLength > 0 ? dirtyBytesStart : 0;
		dirtyBytesLock.unlock();
		
		//Offset in block is equal to the start offset of the current segment + the start of the dirty bytes
		int offset = ((DataSegmentID)this.id).segmentInBlock * Constants.segmentSizeBytes + offsetInSegment;
		
		assert offset <= Constants.dataBlockSizeBytes;
		
		return offset;
	}
	
	public void adjustDirtyOffsets(int currentAppendPosition, int length) {
		dirtyBytesLock.lock();
		
		if(currentAppendPosition < dirtyBytesStart) { //This will be true when the segment is loaded and no append is done yet
			dirtyBytesStart = currentAppendPosition;
		}
		
		if (dirtyBytesStart + dirtyBytesLength < currentAppendPosition + length) //If the previous dirty bytes have not been saved locally
			dirtyBytesLength += (currentAppendPosition + length) - (dirtyBytesStart + dirtyBytesLength);
		
		dirtyBytesLock.unlock();
	}
	
	@Override
	int memorySizeBytes() {
		return Constants.segmentSizeBytes + 8; //FIXME: Get the exact number
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
	public void cleanup() throws IOException {
		bytes = null;
	}

	@Override
	String name() {
		//return Commons.uuidToString(id.uuidHigh, id.uuidLow);
		DataSegmentID id = (DataSegmentID)id();
		return name(id.highBits, id.lowBits, id.segmentInBlock);
	}
	
	static String name(long inumber, long blockNumber, int segmentNumber){
		//return Commons.uuidToString(uuidHigh, uuidLow);
		return String.format("%016x-%016x-%08x", inumber, blockNumber, segmentNumber);
	}
	
	@Override
	int blockSize(){
		return Constants.segmentSizeBytes;
	}
	
	@Override
	String localPath() {
		return localPath(id);
	}
	
	private static String localPath(BlockID id) {
		//TODO: May be a better approach is to use Base64 encoding for the inumber and use hex values
		//for the block number! Currently it is implemented as the encoding of both values.
		
		//String uuid = String.format("%016x%016x", id.highBits, id.lowBits);
		
		String uuid = Commons.uuidToString(id.highBits, id.lowBits);
		int uuidLen = uuid.length();
		
		int wordSize = 3; //Number of characters of the Base64 encoding that make a directory
		int levels = 6; //Number of directory levels //FIXME: we will have 64^3 files in a directory, 
						//which is 262144. This may slow down the underlying filesystem.
		
		assert wordSize * levels < uuidLen-1;
		
		StringBuilder path = new StringBuilder(Constants.blocksPath.length()+uuidLen+levels);
		path.append(Constants.blocksPath + File.separator);
		int rootLen = uuidLen - levels*wordSize;
		path.append(uuid.substring(0, rootLen));
		
		for (int i=0; i<levels; i++){
			path.append(File.separator).append(uuid.substring(rootLen+i*wordSize, rootLen+i*wordSize+wordSize));
		}
		//path.append(File.separator).append(uuid.substring(levels*wordSize));
		
		return path.toString();
	}

	@Override
	public String toString(){
		return id.highBits+"-"+id.lowBits+"-"+name();
	}
	
	public static int recordsPerSegment(int recordSize) {
		return Constants.segmentSizeBytes / recordSize;
	}
	
	public static long segmentInFile(long offsetInFile, int recordSize) {
		// segmentInFile = recordInFile / recordsPerSegment
		return (offsetInFile/recordSize) / recordsPerSegment(recordSize); 
	}
	
	public static long blockInFile(long offsetInFile, int recordSize) {
		// blockInFile = segmentInFile * segmentSize / blockSize
		return segmentInFile(offsetInFile, recordSize) * Constants.segmentSizeBytes / Constants.dataBlockSizeBytes;
	}
	
	public static int segmentInBlock(long offsetInFile, int recordSize) {
		// segmentInBlock = segmentInFile % segmentsPerBlock
		return (int)(segmentInFile(offsetInFile, recordSize) % Constants.segmentsPerBlock);
	}
	
	public static int recordInSegment(long offsetInFile, int recordSize) {
		return (int)(offsetInFile/recordSize) % recordsPerSegment(recordSize);
	}
	
	public static int offsetInSegment(long offsetInFile, int recordSize) {
		// offsetInSegment = recordInSegment + offsetInRecord
		return (int)(recordInSegment(offsetInFile, recordSize) + (offsetInFile % recordSize));
	}
	
	private static int offsetInBlock(long offsetInFile, int recordSize) {
		// offsetInBlock = segmentInBlock + recordInSegment + offsetInRecord
		return (int)(segmentInBlock(offsetInFile, recordSize) + recordInSegment(offsetInFile, recordSize) + (offsetInFile % recordSize));
	}
}