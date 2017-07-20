package kawkab.fs.core;

import kawkab.fs.commons.Constants;
import kawkab.fs.core.Block.BlockType;

public class InodesBlockID extends BlockID{
	public InodesBlockID(int blockIndex) {
		super(Constants.inodesBlocksUuidHigh, blockIndex, InodesBlock.name(blockIndex) ,BlockType.InodeBlock);
	}

	@Override
	public Block newBlock() {
		return new InodesBlock((int)lowBits);
	}
}
