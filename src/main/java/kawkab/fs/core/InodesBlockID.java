package kawkab.fs.core;

import java.io.File;

import kawkab.fs.commons.Constants;

public final class InodesBlockID extends BlockID{
	private final int blockIndex;
	private String localPath;
	
	public InodesBlockID(int blockIndex) {
		super(BlockType.INODES_BLOCK);
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
	
	/*public static String name(int blockIndex) {
		return namePrefix+blockIndex;
	}*/

	/*@Override
	public String name() {
		return this.uniqueKey();
	}*/

	@Override
	public String localPath() {
		if (localPath == null)
			localPath = Constants.inodeBlocksPath + File.separator + (blockIndex/Constants.inodeBlocksPerDirectory) + File.separator + blockIndex;
		
		return localPath;
	}
	
	@Override
	public int perBlockKey() {
		return blockIndex;
	}
	
	@Override
	public String toString() {
		return "I"+blockIndex;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 11; //randomly selected
		int result = 19; //randomly selected
		result = prime * result + blockIndex;
		result = prime * result + ((type == null) ? 0 : (type.ordinal()*17)); //17 is just a random prime
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
		InodesBlockID other = (InodesBlockID) obj;
		if (blockIndex != other.blockIndex)
			return false;
		if (type != other.type)
			return false;
		return true;
	}
}
