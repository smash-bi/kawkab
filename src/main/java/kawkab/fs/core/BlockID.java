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
	
	
	private final String key; // A globally unique key based on the unique name of the block
	private final BlockType type;
	
	public BlockID(String key, BlockType type){
		this.key = key;
		this.type = type;
	}
	
	public BlockID(BlockID id) {
		this.key = id.key;
		this.type = id.type;
	}
	
	abstract public Block newBlock();
	
	abstract public int primaryNodeID();
	
	abstract public String name();
	
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
	public String uniqueKey() {
		return key;
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
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((key == null) ? 0 : key.hashCode());
		result = prime * result + ((type == null) ? 0 : type.hashCode());
		return result;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		BlockID other = (BlockID) obj;
		if (key == null) {
			if (other.key != null)
				return false;
		} else if (!key.equals(other.key))
			return false;
		if (type != other.type)
			return false;
		return true;
	}
	
	
}
