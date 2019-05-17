package kawkab.fs.core;

import kawkab.fs.commons.Configuration;

/**
 * This class and its subclasses should be immutable.
 * 
 * The objects of this class should not last longer than one operation. BlockIDs should not be persisted. These IDs
 * contain information that should be considered valid only at the time of the file operation.
 */
public abstract class BlockID {
	private static final int thisNodeID = Configuration.instance().thisNodeID;
	
	public enum BlockType {
		DATA_SEGMENT, 
		INODES_BLOCK, 
		IBMAP_BLOCK;
		//public static final BlockType values[] = values();
	}
	
	//protected final long k1, k2, k3; //To create a unique key across Ibmps, InodeBlocks, and DataSegments
	//private final String key; // A globally unique key based on the unique name of the block
	protected final BlockType type;
	
	public BlockID(BlockType type){
		this.type = type;
	}
	
	public BlockID(BlockID id) {
		this.type = id.type;
	}
	
	abstract public Block newBlock();
	
	abstract public int primaryNodeID();
	
	abstract public String localPath();
	
	/**
	 * @return an integer that is unique across the block type. For example, the segments of the same block has the 
	 * same perBlockKey.
	 * 
	 * Keys are not unique across block types.
	 */
	abstract public int perBlockKey();
	
	/**
	 * Determines whether the node running this code is the primary writer of this block. 
	 * @return
	 */
	public boolean onPrimaryNode() {
		return primaryNodeID() == thisNodeID;
	}

	/**
	 * @return Type of the block that is represented by this ID
	 */
	public BlockType type() {
		return type;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public abstract int hashCode();

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public abstract boolean equals(Object obj);
	
	
}
