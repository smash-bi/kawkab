package kawkab.fs.core;

import kawkab.fs.commons.Constants;

/**
 * This class and its subclasses should be immutable.
 * 
 * The objects of this class should not last longer than one operation. BlockIDs should not be persisted. These IDs
 * contain information that should be considered valid only at the time of the file operation.
 */
public abstract class BlockID {
	public enum BlockType {
		DATA_SEGMENT, 
		INODES_BLOCK, 
		IBMAP_BLOCK;
		public static final BlockType values[] = values();
	}
	
	//protected final long k1, k2, k3; //To create a unique key across Ibmps, InodeBlocks, and DataSegments
	//private final String key; // A globally unique key based on the unique name of the block
	protected final BlockType type;
	
	public BlockID(BlockType type){
		//this.key = key;
//		this.k1 = k1;
//		this.k2 = k2;
//		this.k3 = k3;
		this.type = type;
	}
	
	public BlockID(BlockID id) {
		//this.key = id.key;
//		this.k1 = id.k1;
//		this.k2 = id.k2;
//		this.k3 = id.k3;
		this.type = id.type;
	}
	
	abstract public Block newBlock();
	
	abstract public int primaryNodeID();
	
	//abstract public String name();
	
	abstract public String localPath();
	
	/**
	 * @return an integer that is unique across the block type. For example, the segments of the same block has the 
	 * same perBlockKey.
	 * 
	 * Keys are not unique across block types.
	 */
	abstract public int perBlockKey(); 
	
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
	/*public String uniqueKey() {
		return key;
	}*/

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
