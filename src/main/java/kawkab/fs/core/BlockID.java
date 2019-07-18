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
		IBMAP_BLOCK,
		INDEX_BLOCK;
		//public static final BlockType values[] = values();
	}
	
	//protected final long k1, k2, k3; //To create a unique key across Ibmps, InodeBlocks, and DataSegments
	//private final String key; // A globally unique key based on the unique name of the block
	protected final BlockType type;
	
	/**
	 * @param type Type of the block of this ID
	 */
	public BlockID(BlockType type){
		this.type = type;
	}
	
	/**
	 * Creates a new block of the type corresponding to this ID.
	 * @return the reference to they new block
	 */
	abstract public Block newBlock();
	
	/**
	 * @return ID of the primary node of this block
	 */
	abstract public int primaryNodeID();
	
	/**
	 *
	 * @return Path of this block's file in the local storage
	 */
	abstract public String localPath();
	
	/**
	 * @return an integer that is unique across the block type. For example, the segments of the same block has the
	 * same perBlockTypeKey.
	 *
	 * Keys are not unique across block types.
	 */
	abstract public int perBlockTypeKey();
	
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
	
	/**
	 * Returns a unique ID of the file associated with the block. Note that this ID is different than the actual file name
	 * used in the local and global storage systems. Those modules use localPath() functions to store the files.
	 * @return ID
	 */
	public abstract String fileID();

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
