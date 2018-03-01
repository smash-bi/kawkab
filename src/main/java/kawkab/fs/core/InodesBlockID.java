package kawkab.fs.core;

import java.io.File;

import kawkab.fs.commons.Constants;

public final class InodesBlockID extends BlockID{
	private final static String namePrefix = "I";
	private final int blockIndex;
	
	public InodesBlockID(int blockIndex) {
		super(name(blockIndex) ,BlockType.InodeBlock);
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
	public boolean areEqual(BlockID blockID){
		if (blockID == null)
			return false;
		
		if (!(blockID instanceof InodesBlockID))
			return false;
		
		InodesBlockID id;
		try {
			id = (InodesBlockID) blockID;
		}catch(Exception e){
			return false;
		}
		
		return blockIndex == id.blockIndex;
	}
	
	@Override
	public String toString() {
		return name(blockIndex);
	}
}
