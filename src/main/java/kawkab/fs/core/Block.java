package kawkab.fs.core;

import java.nio.ByteBuffer;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import kawkab.fs.core.exceptions.InsufficientResourcesException;

public abstract class Block implements AutoCloseable {
	public enum BlockType {
		DataBlock, InodeBlock, IbmapBlock
	}
	
	protected Lock lock;
	protected boolean dirty;
	protected final BlockID id;
	protected Cache cache = Cache.instance();
	
	/**
	 * Acquire the values of this block from the given Buffer. This function loads values of the
	 * class variables from the given ByteBuffer. It consumes the number of bytes equal to the
	 * blockSize() function.
	 * @param buffer
	 * @throws InsufficientResourcesException
	 */
	abstract void fromBuffer(ByteBuffer buffer) throws InsufficientResourcesException;
	
	/**
	 * Serializes this block in the given buffer.
	 * @param buffer
	 * @throws InsufficientResourcesException
	 */
	abstract void toBuffer(ByteBuffer buffer) throws InsufficientResourcesException;
	
	/**
	 * @return Returns the name of the current block, which can be used as a unique key.
	 */
	abstract String name();
	
	/**
	 * @return The path of the block local to the current machine, which may be different from the
	 * path global to the distributed filesystem. The returned path is an absolute file path.
	 */
	abstract String localPath();
	
	/**
	 * The serialization/deserialization size of this block.
	 * 
	 * @return Returns the size of the block in bytes. When the block is serializes or deserializes,
	 * it consumes the number of bytes equal to the returned value of this function.  
	 */
	abstract int blockSize();
	
	//FIXME: There must be some better mechanism to achieve this. 
	public static Block newBlock(BlockID blockID) {
		//FIXME: Should we use visitor pattern here? This code is not different than using instanceof
		Block block;
		BlockType type = blockID.type;
		if (type == BlockType.IbmapBlock) {
			block = new Ibmap((int)blockID.uuidLow);
		} else if (type == BlockType.InodeBlock) {
			block = new InodesBlock((int)blockID.uuidLow);
		} else if (type == BlockType.DataBlock) {
			block = new DataBlock(blockID);
		} else {
			throw new IllegalArgumentException("Block type is invalid: " + type);
		}
		
		return block;
	}
	
	protected Block(BlockID id) {
		this.id = id;
		lock = new ReentrantLock();
	}
	
	/**
	 * Clears the dirty bit of this block.
	 */
	void clearDirty(){
		dirty = false;
	}
	
	/**
	 * @return Returns whether this block has been modified until now or not.
	 */
	boolean dirty(){
		return dirty;
	}
	
	void markDirty(){
		dirty = true;
	}
	
	public void lock(){
		lock.lock();
	}
	
	public void unlock(){
		lock.unlock();
	}
	
	public BlockID id(){
		return id;
	}
	
	/**
	 * This function causes the cache to flush this block if the block is dirty. This function
	 * does not closes this block from writing. It overrides the close function in the AutoCloseable
	 * interface.
	 */
	@Override
	public void close(){
		cache.releaseBlock(this.id());
	}
}