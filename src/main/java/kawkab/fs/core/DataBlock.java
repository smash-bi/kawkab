package kawkab.fs.core;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.UUID;

import org.agrona.IoUtil;

import kawkab.fs.commons.Commons;
import kawkab.fs.commons.Constants;
import kawkab.fs.core.exceptions.InsufficientResourcesException;
import kawkab.fs.core.exceptions.InvalidFileOffsetException;

public class DataBlock extends Block {
	private MappedByteBuffer buffer;
	
	public long blockNumber; //FIXME: Used for debugging only.
	
	
	/**
	 * The constructor should not create a new file in the underlying filesystem. This constructor
	 * does not reads data from the underlying file. Instead, use fromBuffer() function for that
	 * purpose. 
	 * @param uuid
	 */
	DataBlock(BlockID uuid){
		super(uuid, BlockType.DataBlock);
		//System.out.println(" Opened block: " + name());
	}
	
	/**
	 * This function creates a new block ID and the new file, if required, in the underlying
	 * filesystem. This function should not use a block of memory as this memory competes with
	 * the cache memroy.
	 * @return
	 */
	static BlockID createNewBlock() {
		BlockID id = randomID();
		File file = new File(localPath(id));
		IoUtil.createEmptyFile(file, Constants.dataBlockSizeBytes, false);
		
		return id;
	}
	
	/**
	 * @param data Data to be appended
	 * @param offset  Offset in data
	 * @param length  Number of bytes to append: data[offset] to data[offset+length-1] inclusive.
	 * @param offsetInFile  
	 * @return number of bytes appended
	 */
	synchronized int append(byte[] data, int offset, int length, long offsetInFile){
		assert offset >= 0;
		assert offset < data.length;
		
		int offsetInBlock = (int)(offsetInFile % Constants.dataBlockSizeBytes);
		int capacity = Constants.dataBlockSizeBytes - offsetInBlock;
		
		int toAppend = length <= capacity ? length : capacity;
		
		buffer.position(offsetInBlock);
		buffer.put(data, offset, toAppend);
		
		/*try {
			file.seek(offsetInBlock);
			//System.out.println(String.format("%d, %d, %d, %d, %d, %d, %d", offset, length, capacity, toAppend, offsetInBlock, file.length(), file.getFilePointer()));
			file.write(data, offset, toAppend);
		} catch (IOException e) { //FIXME: Handle exception.
			e.printStackTrace();
		}*/
		
		/*for(int i=0; i<toAppend; i++){
			this.data[offsetInBlock] = data[i+offset];
			offsetInBlock++;
		}*/
		
		//FIXME: Do we need to grab a lock to mark the data block as dirty???
		//lock.lock();
		dirty = true;
		//lock.unlock();
		
		//System.out.println("  Append: " + name() + " - " + toAppend);
		
		return toAppend;
	}
	
	synchronized int appendLong(long data, long offsetInFile){
		int offsetInBlock = (int)(offsetInFile % Constants.dataBlockSizeBytes);
		int capacity = Constants.dataBlockSizeBytes - offsetInBlock;
		int longSize = Long.BYTES;
		if (capacity < longSize)
			return 0;
		
		/*ByteBuffer buffer = ByteBuffer.wrap(this.data);
		buffer.position(offsetInBlock);
		buffer.putLong(data);*/
		
		/*try {
			file.seek(offsetInBlock);
			file.writeLong(data);
		} catch (IOException e) { //FIXME: Handle exception.
			e.printStackTrace();
		}*/
		
		buffer.putLong(offsetInBlock, data);
		
		//FIXME: Do we need to grab a lock to mark the data block as dirty???
		//lock.lock();
		dirty = true;
		//lock.unlock();
		
		return longSize;
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
		
		/*ByteBuffer buffer = ByteBuffer.wrap(this.data);
		buffer.position(offsetInBlock);
		return buffer.getLong();*/
		
		//file.seek(offsetInBlock);
		//return file.readLong();
		
		return buffer.getLong(offsetInBlock);
	}
	
	synchronized int appendInt(int data, long offsetInFile){
		int offsetInBlock = (int)(offsetInFile % Constants.dataBlockSizeBytes);
		int capacity = Constants.dataBlockSizeBytes - offsetInBlock;
		int intSize = Integer.BYTES;
		if (capacity < intSize)
			return 0;
		
		/*ByteBuffer buffer = ByteBuffer.wrap(this.data);
		buffer.position(offsetInBlock);
		buffer.putInt(data);*/
		
		/*try {
			file.seek(offsetInBlock);
			file.writeInt(data);
		} catch (IOException e) { //FIXME: Exception
			e.printStackTrace();
		}*/
		
		buffer.putInt(offsetInBlock, data);
		
		//FIXME: Do we need to grab a lock to mark the data block as dirty???
		//lock.lock();
		dirty = true;
		//lock.unlock();
		
		return intSize;
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
		
		/*ByteBuffer buffer = ByteBuffer.wrap(this.data);
		buffer.position(offsetInBlock);
		return buffer.getInt();*/
		
		//file.seek(offsetInBlock);
		//return file.readInt();
		
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
	synchronized int read(byte[] dstBuffer, int dstBufferOffset, int length, long offsetInFile) throws InvalidFileOffsetException, IOException{
		//int blockOffset = (int)(fileOffsetBytes % Constants.dataBlockSizeBytes);
		
		int blockSize = Constants.dataBlockSizeBytes;
		int offsetInBlock = (int)(offsetInFile % Constants.dataBlockSizeBytes);
		if (offsetInBlock >= blockSize || offsetInBlock < 0) {
			throw new InvalidFileOffsetException(
					String.format("Given file offset %d is outside of the block. Block size = %d bytes.",
					offsetInBlock, blockSize));
		}
		
		int readSize = offsetInBlock+length <= blockSize ? length : blockSize-offsetInBlock;
		/*for(int i=0; i<readSize; i++){
			dstBuffer[bufferOffset+i] = this.data[offsetInBlock+i];
		}*/
		
		//file.seek(offsetInBlock);
		//file.read(dstBuffer, dstBufferOffset, readSize);
		
		buffer.position(offsetInBlock);
		buffer.get(dstBuffer, dstBufferOffset, readSize);
		
		return readSize;
	}
	
	/*long firstAppendTime(){
		return firstAppendTime;
	}
	
	long lastAppendTime(){
		return lastAppendTime;
	}*/
	
	/*boolean hasByte(long offsetInFile){
		return false;
	}*/
	
	BlockID uuid(){
		return id;
	}
	
	/*byte[] data(){
		return data;
	}*/
	
	@Override
	void fromBuffer(ByteBuffer buffer) throws InsufficientResourcesException {
		/*int blockSize = Constants.dataBlockSizeBytes;
		if (buffer.remaining() < blockSize)
			throw new InsufficientResourcesException(String.format("Buffer has less bytes remaining: "
					+ "%d bytes are remaining, %d bytes are required.",buffer.remaining(),blockSize));
		
		data = new byte[Constants.dataBlockSizeBytes];
		buffer.get(data);*/
	}

	@Override
	void toBuffer(ByteBuffer buffer) throws InsufficientResourcesException {
		/*int blockSize = Constants.dataBlockSizeBytes;
		if (buffer.capacity() < blockSize)
			throw new InsufficientResourcesException(String.format("Buffer capacity is less than "
						+ "required: Capacity = %d bytes, required = %d bytes.",buffer.capacity(),blockSize));
		
		buffer.put(data);*/
	}
	
	@Override
	public void loadFromDisk() {
		File location = new File(localPath());
		buffer = IoUtil.mapExistingFile(location, id.key, 0, Constants.dataBlockSizeBytes);
	}
	
	@Override
	public void storeToDisk(){
		IoUtil.unmap(buffer);
		//System.out.println("\tClosed block: " + name());
	}

	@Override
	String name() {
		return Commons.uuidToString(id.uuidHigh, id.uuidLow);
	}
	
	static String name(long uuidHigh, long uuidLow){
		return Commons.uuidToString(uuidHigh, uuidLow);
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
		String uuid = Commons.uuidToString(id.uuidHigh, id.uuidLow);
		int uuidLen = uuid.length();
		
		int wordSize = 2; //Number of characters of the Base64 encoding that make a directory
		int levels = 3; //Number of directory levels
		
		StringBuilder path = new StringBuilder(Constants.blocksPath.length()+uuidLen+levels);
		path.append(Constants.blocksPath);
		for (int i=0; i<levels; i++){
			path.append(File.separator).append(uuid.substring(i*wordSize, i*wordSize+wordSize));
		}
		path.append(File.separator).append(uuid.substring(levels*wordSize));
		
		return path.toString();
	}
	
	public static BlockID randomID(){
		UUID uuid = UUID.randomUUID();
		long uuidHigh = uuid.getMostSignificantBits();
		long uuidLow = uuid.getLeastSignificantBits();
		return new BlockID(uuidHigh, uuidLow, DataBlock.name(uuidHigh, uuidLow), BlockType.DataBlock);
	}
	
	@Override
	public String toString(){
		return blockNumber+"-"+name();
	}
	
	private void createIfNotExist(){
		File file = new File(localPath());
		if (!file.exists()) {
			file.getParentFile().mkdirs();
			try {
				file.createNewFile();
				RandomAccessFile raf= new RandomAccessFile(file, "rw");
				raf.setLength(Constants.dataBlockSizeBytes);
				raf.close();
				//System.out.println("  Created block: " + name());
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}