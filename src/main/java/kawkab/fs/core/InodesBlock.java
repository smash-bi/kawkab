package kawkab.fs.core;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import kawkab.fs.commons.Constants;
import kawkab.fs.core.exceptions.InsufficientResourcesException;
import kawkab.fs.core.exceptions.InvalidArgumentsException;
import kawkab.fs.core.exceptions.InvalidFileOffsetException;
import kawkab.fs.core.exceptions.MaxFileSizeExceededException;
import kawkab.fs.core.exceptions.OutOfMemoryException;

public class InodesBlock extends Block {
	private final int blockIndex; //Not saved persistently
	private boolean dirty; //Not saved persistently
	private Inode[] inodes;
	private static boolean bootstraped; //Not saved persistently
	
	InodesBlock(int blockIndex){
		this.blockIndex = blockIndex;
	}
	
	public int append(long iNumber, byte[] data, int offset, int length) throws OutOfMemoryException, 
								MaxFileSizeExceededException, InvalidFileOffsetException {
		int inodeNumber = (int)(iNumber % Constants.inodesPerBlock);
		int ret = inodes[inodeNumber].append(data, offset, length);
		dirty = inodes[inodeNumber].dirty();
		return ret;
	}
	
	public int read(long iNumber, byte[] buffer, int length, long offsetInFile) throws 
			InvalidFileOffsetException, InvalidArgumentsException {
		int inodeNumber = (int)(iNumber % Constants.inodesPerBlock);
		return inodes[inodeNumber].read(buffer, length, offsetInFile);
	}
	
	public long fileSize(long inumber){
		int inodeNumber = (int)(inumber % Constants.inodesPerBlock);
		return inodes[inodeNumber].fileSize();
	}
	
	@Override
	void fromBuffer(ByteBuffer buffer) throws InsufficientResourcesException{
		int blockSize = Constants.inodesBlockSizeBytes;
		if (buffer.remaining() < blockSize)
			throw new InsufficientResourcesException(String.format("Buffer has less bytes remaining: "
					+ "%d bytes are remaining, %d bytes are required.",buffer.remaining(),blockSize));
		
		inodes = new Inode[Constants.inodesPerBlock];
		for(int i=0; i<Constants.inodesPerBlock; i++){
			inodes[i] = Inode.fromBuffer(buffer);
		}
	}
	
	@Override
	void toBuffer(ByteBuffer buffer) throws InsufficientResourcesException{
		int blockSize = Constants.inodesBlockSizeBytes;
		if (buffer.capacity() < blockSize)
			throw new InsufficientResourcesException(String.format("Buffer capacity is less than "
						+ "required: Capacity = %d bytes, required = %d bytes.",buffer.capacity(),blockSize));
		
		for(Inode inode : inodes) {
			inode.toBuffer(buffer);
		}
	}
	
	@Override
	boolean dirty(){
		return dirty;
	}
	
	@Override
	void clearDirty() {
		dirty = false;
	}

	@Override
	String name() {
		return name(blockIndex);
	}
	
	static String name(int blockIndex){
		return "inb"+blockIndex;
	}
	
	@Override
	String localPath(){
		return Constants.inodeBlocksPath + "/" + (blockIndex/Constants.inodeBlocksPerDirectory) + "/" + name(blockIndex);
	}
	
	@Override
	int blockSize(){
		return Constants.inodesBlockSizeBytes;
	}
	
	static void bootstrap() throws IOException{
		if (bootstraped)
			return;
		
		File folder = new File(Constants.inodeBlocksPath);
		if (!folder.exists()){
			System.out.println("  Creating folder: " + folder.getAbsolutePath());
			folder.mkdirs();
		}
		
		LocalStore storage = LocalStore.instance();
		int offset = Constants.inodesBlocksRangeStart;
		for(int i=0; i<Constants.inodeBlocksPerMachine; i++){
			InodesBlock block = new InodesBlock(offset+i);
			block.inodes = new Inode[Constants.inodesPerBlock];
			for (int j=0; j<Constants.inodesPerBlock; j++) {
				block.inodes[j] = Inode.bootstrap();
			}
			
			File file = new File(block.localPath());
			if (!file.exists()){
				storage.writeBlock(block);
			}
		}
		
		bootstraped = true;
	}
	
	static void shutdown(){
		System.out.println("Closing InodesBlock");
		//TODO: Stop new requests
	}
}
