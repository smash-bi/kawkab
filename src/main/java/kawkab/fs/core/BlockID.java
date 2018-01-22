package kawkab.fs.core;

import kawkab.fs.commons.Constants;
import kawkab.fs.core.Block.BlockType;

/**
 * This class and its subclasses should be immutable.
 */
public abstract class BlockID {
	//All the public fields in this class must be made immutable.
	public final long highBits;
	public final long lowBits;
	public final String key;
	public final BlockType type;
	
	public BlockID(long highBits, long lowBits, String key, BlockType type){
		this.highBits = highBits;
		this.lowBits = lowBits;
		this.key = key;
		this.type = type;
	}
	
	public BlockID(BlockID id) {
		this.highBits = id.highBits;
		this.lowBits = id.lowBits;
		this.key = id.key;
		this.type = id.type;
	}
	
	@Override
	public boolean equals(Object blockID){
		if (blockID == null)
			return false;
		
		BlockID id;
		try {
			id = (BlockID) blockID;
		}catch(Exception e){
			return false;
		}
		
		return highBits == id.highBits &&
				lowBits == id.lowBits &&
				key.equals(id.key) &&
				type == id.type;
	}
	
	@Override
	public String toString() {
		return highBits+"-"+lowBits+"-"+name();
	}

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
