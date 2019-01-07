package kawkab.fs.core;

import java.io.File;

import kawkab.fs.commons.Constants;

public final class InodesBlockID extends BlockID{
	private final static String namePrefix = "I";
	private final int blockIndex;
	
	public InodesBlockID(int blockIndex) {
		super(name(blockIndex) ,BlockType.INODES_BLOCK);
		this.blockIndex = blockIndex;
	}
	
	public int blockIndex() {
		return blockIndex;
	}
	
	@Override
	public int primaryNodeID() {
		return (int) (blockIndex / Constants.inodeBlocksPerMachine); //First block has ID 0
	}

	@Override
	public Block newBlock() {
		return new InodesBlock(this);
	}
	
	public static String name(int blockIndex) {
		return namePrefix+blockIndex;
	}

	@Override
	public String name() {
		return name(blockIndex);
	}

	@Override
	public String localPath() {
		return Constants.inodeBlocksPath + File.separator + 
				(blockIndex/Constants.inodeBlocksPerDirectory) + File.separator + name(blockIndex);
	}
	
	@Override
	public int perBlockKey() {
		return blockIndex;
	}
	
	@Override
	public String toString() {
		return name(blockIndex);
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + blockIndex;
		return result;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		InodesBlockID other = (InodesBlockID) obj;
		if (blockIndex != other.blockIndex)
			return false;
		return true;
	}
}
