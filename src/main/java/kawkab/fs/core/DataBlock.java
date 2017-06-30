package kawkab.fs.core;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.Set;

import kawkab.fs.commons.Commons;
import kawkab.fs.commons.Constants;
import kawkab.fs.core.exceptions.InsufficientResourcesException;
import kawkab.fs.core.exceptions.InvalidFileOffsetException;

public class DataBlock extends Block {
	//private MappedByteBuffer buffer;
	private SeekableByteChannel channel;
	
	public long blockNumber; //FIXME: Used for debugging only.
	
	
	/**
	 * The constructor should not create a new file in the underlying filesystem. This constructor
	 * does not reads data from the underlying file. Instead, use fromBuffer() or laodFromDisk() 
	 * function for that purpose. 
	 * @param uuid
	 */
	DataBlock(BlockID uuid){
		super(uuid, BlockType.DataBlock);
		//System.out.println(" Opened block: " + name());
	}
	
	/**
	 * This function creates a new block ID and the new file, if required, in the underlying
	 * filesystem. This function should not use a block of memory as this memory competes with
	 * the cache memory.
	 * 
	 * @return ID of the newly created block.
	 * @throws IOException 
	 */
	static BlockID createNewBlock(long inumber, long blockNumber) throws IOException {
		//Todo: Acquire a block from disk
		
		BlockID id = newID(inumber, blockNumber);
		File file = new File(localPath(id));
		
		if (file.exists()) {
			//throw new IOException("Block file already exists: " + file.getAbsolutePath()); //FIXME: Throw exception if the file already exists.
			return id;
		}
		
		File parent = file.getParentFile();
		if (!parent.exists()) {
			if (!parent.mkdirs()) {
				throw new IOException("Unable to create directories: "+parent.getAbsolutePath());
			}
		}
		
		if (!file.createNewFile()) {
			throw new IOException("Unable to create block file: "+file.getAbsolutePath());
		}
		
		return id;
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
		
		int offsetInBlock = (int)(offsetInFile % Constants.dataBlockSizeBytes);
		int capacity = Constants.dataBlockSizeBytes - offsetInBlock;
		
		int toAppend = length <= capacity ? length : capacity;
		
		//buffer.position(offsetInBlock);
		//buffer.put(data, offset, toAppend);
		
		ByteBuffer buffer = ByteBuffer.wrap(data, offset, toAppend);
		
		channel.position(offsetInBlock);
		
		//long t = System.nanoTime();
		
		int written = channel.write(buffer);
		
		//long elapsed = (System.nanoTime() - t)/1000;
		//System.out.println("elaped: " + elapsed);
		
		//FIXME: Do we need to grab a lock to mark the data block as dirty???
		//lock.lock();
		dirty = true;
		//lock.unlock();
		
		//System.out.println("  Append: " + name() + " - " + toAppend);
		
		return written;
	}
	
	synchronized int writeLong(long data, long offsetInFile) throws IOException{
		int offsetInBlock = (int)(offsetInFile % Constants.dataBlockSizeBytes);
		int capacity = Constants.dataBlockSizeBytes - offsetInBlock;
		int longSize = Long.BYTES;
		if (capacity < longSize)
			return 0;
		
		ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
		buffer.putLong(data);
		buffer.flip();
		
		channel.position(offsetInFile);
		int written = channel.write(buffer);
		
		//FIXME: Do we need to grab a lock to mark the data block as dirty???
		//lock.lock();
		dirty = true;
		//lock.unlock();
		
		return written;
	}
	
	synchronized long readLong(long offsetInFile) throws InvalidFileOffsetException, IOException{
		//int blockOffset = (int)(fileOffsetBytes % Constants.dataBlockSizeBytes);
		
		int offsetInBlock = (int)(offsetInFile % Constants.dataBlockSizeBytes);
		int blockSize = Constants.dataBlockSizeBytes;
		
		if (offsetInBlock >= blockSize || 
				offsetInBlock+Long.BYTES > blockSize || offsetInBlock < 0)
			throw new InvalidFileOffsetException(
					String.format("File Offset %d is invalid. Data block size = %d bytes.", 
							offsetInBlock, blockSize));
		
		ByteBuffer dst = ByteBuffer.allocate(Long.BYTES);
		channel.position(offsetInBlock);
		channel.read(dst);
		dst.flip();
		return dst.getLong();
		//return buffer.getLong(offsetInBlock);
	}
	
	synchronized int writeInt(int data, long offsetInFile) throws IOException{
		int offsetInBlock = (int)(offsetInFile % Constants.dataBlockSizeBytes);
		int capacity = Constants.dataBlockSizeBytes - offsetInBlock;
		int intSize = Integer.BYTES;
		if (capacity < intSize)
			return 0;
		
		//buffer.putInt(offsetInBlock, data);
		
		ByteBuffer src = ByteBuffer.allocate(Integer.BYTES);
		src.putInt(data);
		src.flip();
		channel.position(offsetInBlock);
		int written = channel.write(src);
		
		//FIXME: Do we need to grab a lock to mark the data block as dirty???
		//lock.lock();
		dirty = true;
		//lock.unlock();
		
		return written;
	}
	
	synchronized int readInt(long offsetInFile) throws InvalidFileOffsetException, IOException{
		//int blockOffset = (int)(fileOffsetBytes % Constants.dataBlockSizeBytes);
		
		int blockSize = Constants.dataBlockSizeBytes;
		int offsetInBlock = (int)(offsetInFile % Constants.dataBlockSizeBytes);
		if (offsetInBlock >= blockSize || 
				offsetInBlock+4 >= blockSize || offsetInBlock < 0) {
			throw new InvalidFileOffsetException(
					String.format("File Offset %d is invalid. Data block size = %d bytes.", 
							offsetInBlock, blockSize));
		}
		
		//file.seek(offsetInBlock);
		//return file.readInt();
		
		ByteBuffer dst = ByteBuffer.allocate(Integer.BYTES);
		channel.position(offsetInBlock);
		channel.read(dst);
		dst.flip();
		return dst.getInt();
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
	synchronized int read(byte[] dstBuffer, int dstBufferOffset, int length, long offsetInFile) throws InvalidFileOffsetException, IOException{
		int blockSize = Constants.dataBlockSizeBytes;
		int offsetInBlock = (int)(offsetInFile % Constants.dataBlockSizeBytes);
		if (offsetInBlock >= blockSize || offsetInBlock < 0) {
			throw new InvalidFileOffsetException(
					String.format("Given file offset %d is outside of the block. Block size = %d bytes.",
					offsetInBlock, blockSize));
		}
		
		
		
		int readSize = offsetInBlock+length <= blockSize ? length : blockSize-offsetInBlock;
		ByteBuffer dst = ByteBuffer.wrap(dstBuffer);
		dst.position(dstBufferOffset);
		dst.mark();
		dst.limit(dstBufferOffset+readSize);
		channel.position(offsetInBlock);
		
		int readBytes = channel.read(dst);
		
		return readBytes;
	}
	
	BlockID uuid(){
		return id;
	}
	
	@Override
	void fromBuffer(ByteBuffer buffer) throws InsufficientResourcesException {
		//We don't have to anything here because the loadFromDisk function implemented in this class does not call fromBuffer method. 
	}

	@Override
	void toBuffer(ByteBuffer buffer) throws InsufficientResourcesException {
		//We don't have to anything here because the storeToDisk function implemented in this class does not call toBuffer method.
	}
	
	@Override
	public void loadFromDisk() throws IOException {
		//System.out.println("Opening block: " + id);
		Path path = new File(localPath()).toPath();
		//buffer = IoUtil.mapExistingFile(location, id.key, 0, Constants.dataBlockSizeBytes);
		Set<OpenOption> options = new HashSet<OpenOption>();
	    options.add(StandardOpenOption.WRITE);
	    options.add(StandardOpenOption.READ);
		channel = Files.newByteChannel(path, options);
	}
	
	
	/**
	 * This is the last function called before releasing memory from cache. Therefore, we should also perform
	 * cleanup operations here.
	 */
	@Override
	public void storeToDisk() throws IOException{
		//System.out.println("Closing block: " + id);
		//IoUtil.unmap(buffer);
		//System.out.println("\tClosed block: " + name());
		cleanup();
	}
	
	@Override
	public void cleanup() throws IOException {
		if (channel == null)
			return;
		
		if (channel.isOpen())			
			channel.close();
		
		channel = null;
	}

	@Override
	String name() {
		//return Commons.uuidToString(id.uuidHigh, id.uuidLow);
		return name(id.highBits, id.lowBits);
	}
	
	static String name(long uuidHigh, long uuidLow){
		//return Commons.uuidToString(uuidHigh, uuidLow);
		return String.format("%016x-%016x", uuidHigh, uuidLow);
	}
	
	@Override
	int blockSize(){
		return Constants.dataBlockSizeBytes;
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

	public static BlockID newID(long inumber, long blockNumber){
		//Block ID is a 128 bit number: high order 64 bit are the inode number and the lower order
		//64 bit is the block number.
		
		long uuidHigh = inumber;
		long uuidLow = blockNumber;
		
		return new BlockID(uuidHigh, uuidLow, DataBlock.name(uuidHigh, uuidLow), BlockType.DataBlock);
	}
	
	@Override
	public String toString(){
		return id.highBits+"-"+id.lowBits+"-"+name();
	}
}