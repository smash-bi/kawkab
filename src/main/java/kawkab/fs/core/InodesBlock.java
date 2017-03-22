package kawkab.fs.core;

import java.nio.ByteBuffer;

import javax.naming.InsufficientResourcesException;

import kawkab.fs.commons.Constants;
import kawkab.fs.core.exceptions.InvalidArgumentsException;
import kawkab.fs.core.exceptions.InvalidFileOffsetException;
import kawkab.fs.core.exceptions.MaxFileSizeExceededException;
import kawkab.fs.core.exceptions.OutOfMemoryException;

public class InodesBlock {
	//private int blockNumber;
	private boolean dirty;
	private Inode[] inodes;
	
	private InodesBlock(){}
	
	static InodesBlock bootstrap(int blockNumber){
		InodesBlock blk = new InodesBlock();
		//blk.blockNumber = blockNumber;
		blk.dirty = false;
		blk.inodes = new Inode[Constants.inodesPerBlock];
		for (int i=0; i<Constants.inodesPerBlock; i++) {
			blk.inodes[i] = Inode.bootstrap();
		}
		return blk;
	}
	
	public int append(long iNumber, byte[] data, int offset, int length) throws OutOfMemoryException, 
								MaxFileSizeExceededException, InvalidFileOffsetException {
		int inodeNumber = (int)(iNumber % Constants.inodesPerBlock);
		int ret = inodes[inodeNumber].append(data, offset, length);
		dirty = inodes[inodeNumber].dirty();
		return ret;
	}
	
	public int read(long iNumber, byte[] buffer, int length, long offsetInFile) throws InvalidFileOffsetException, InvalidArgumentsException {
		int inodeNumber = (int)(iNumber % Constants.inodesPerBlock);
		return inodes[inodeNumber].read(buffer, length, offsetInFile);
	}
	
	
	static InodesBlock fromBuffer(ByteBuffer buffer) throws InsufficientResourcesException{
		int blockSize = Constants.inodesPerBlock*Constants.inodeSizeBytes;
		if (buffer.remaining() < blockSize)
			throw new InsufficientResourcesException(String.format("Buffer has less bytes remaining: "
					+ "%d bytes are remaining, %d bytes are required.",buffer.remaining(),blockSize));
		
		//blockNumber = buffer.getInt();
		InodesBlock blk = new InodesBlock();
		blk.inodes = new Inode[Constants.inodesPerBlock];
		for(int i=0; i<Constants.inodesPerBlock; i++){
			blk.inodes[i] = Inode.fromBuffer(buffer);
		}
		return blk;
	}
	
	void toBuffer(ByteBuffer buffer) throws InsufficientResourcesException{
		int blockSize = Constants.inodesPerBlock*Constants.inodeSizeBytes;
		if (buffer.capacity() < blockSize)
			throw new InsufficientResourcesException(String.format("Buffer capacity is less than "
					+ "required: Capacity = %d bytes, required = %d bytes.",buffer.capacity(),blockSize));
		
		//buffer.putInt(blockNumber);
		for(Inode inode : inodes) {
			inode.toBuffer(buffer);
		}
	}
	
	boolean dirty(){
		return dirty;
	}
	
	void clear(){
		dirty = false;
	}
}
