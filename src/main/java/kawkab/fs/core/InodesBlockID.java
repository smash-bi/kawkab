package kawkab.fs.core;

import java.io.File;
import java.util.Objects;

import kawkab.fs.commons.Configuration;

public final class InodesBlockID extends BlockID{
	private final int blockIndex;
	private String localPath;
	
	private static final int inodeBlocksPerDirectory = Configuration.instance().inodeBlocksPerDirectory;
	private static final int inodeBlocksPerMachine = Configuration.instance().inodeBlocksPerMachine;
	private static final String inodeBlocksPath = Configuration.instance().inodeBlocksPath + File.separator;
	
	public InodesBlockID(int blockIndex) {
		super(BlockType.INODES_BLOCK);
		this.blockIndex = blockIndex;
	}
	
	public int blockIndex() {
		return blockIndex;
	}
	
	@Override
	public int primaryNodeID() {
		return (int) (blockIndex / inodeBlocksPerMachine); //First block has ID 0
	}

	@Override
	public Block newBlock() {
		return new InodesBlock(this);
	}

	@Override
	public String localPath() {
		if (localPath == null)
			localPath = inodeBlocksPath + (blockIndex/inodeBlocksPerDirectory) + File.separator + blockIndex;
		
		return localPath;
	}

	@Override
	public int perBlockTypeKey() {
		return hashCode();
	}
	
	@Override
	public String fileID() {
		return "I"+blockIndex;
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
		result = prime * result + 17; //17 is just a random prime to differentiate it from the Ibmap block
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
