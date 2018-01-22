package kawkab.fs.core;

import java.io.File;

import kawkab.fs.commons.Constants;
import kawkab.fs.core.Block.BlockType;

public class InodesBlockID extends BlockID{
	private final static String namePrefix = "inb";
	
	public InodesBlockID(int blockIndex) {
		super(Constants.inodesBlocksUuidHigh, blockIndex, name(blockIndex) ,BlockType.InodeBlock);
	}
	
	@Override
	public int primaryNodeID() {
		return (int) (lowBits / Constants.inodeBlocksPerMachine); //First block has ID 0
	}

	@Override
	public Block newBlock() {
		return new InodesBlock((int)lowBits);
	}
	
	public static String name(int blockIndex) {
		return namePrefix+blockIndex;
	}

	@Override
	public String name() {
		return name((int)lowBits);
	}

	@Override
	public String localPath() {
		return Constants.inodeBlocksPath + File.separator + 
				(lowBits/Constants.inodeBlocksPerDirectory) + File.separator + name((int)lowBits);
	}
}
