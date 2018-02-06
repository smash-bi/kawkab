package kawkab.fs.core;

import kawkab.fs.commons.Constants;

/**
 * This class and its subclasses should be immutable.
 * 
 * The objects of this class should not last longer than one operation. BlockIDs should not be persisted. These IDs
 * contain information that is only valid at the time of the file operation. The values may become invalid across
 * multiple file operations.
 */
public abstract class BlockID {
	public enum BlockType {
		DataBlock,  // A segment in a data block 
		InodeBlock, 
		IbmapBlock;
		public static final BlockType values[] = values();
	}
	
	//All the public fields in this class must be made immutable.
	protected final String key;
	protected final BlockType type;
	
	public BlockID(String key, BlockType type){
		this.key = key;
		this.type = type;
	}
	
	public BlockID(BlockID id) {
		this.key = id.key;
		this.type = id.type;
	}
	
	public abstract boolean areEqual(BlockID blockID);

	abstract public Block newBlock();
	
	abstract public int primaryNodeID();
	
	abstract public String name();
	
	abstract public String localPath();
	
	/**
	 * Determines whether this node is the primary writer of the block. 
	 * @return
	 */
	public boolean onPrimaryNode() {
		return primaryNodeID() == Constants.thisNodeID;
	}

	/**
	 * @return Globally unique string for this ID that can be used in maps
	 */
	public String key() {
		return key;
	}

	/**
	 * @return Type of the block that is represented by this ID
	 */
	public BlockType type() {
		return type;
	}
	
	/*public Block newBlock() {
		//TODO: Use an alternate solution where BlockIDs are of different types and they
		//create only one type of blocks.
		Block block;
		if (type == BlockType.IbmapBlock) {
			block = new Ibmap((int)lowBits);
		} else if (type == BlockType.InodeBlock) {
			block = new InodesBlock((int)lowBits);
		} else if (type == BlockType.DataBlock) {
			block = new DataBlock(this);
		} else {
			throw new IllegalArgumentException("Block type is invalid: " + type);
		}
		
		return block;
	}*/
}
