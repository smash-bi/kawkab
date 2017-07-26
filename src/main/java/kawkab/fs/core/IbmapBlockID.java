package kawkab.fs.core;

import kawkab.fs.commons.Constants;
import kawkab.fs.core.Block.BlockType;

public class IbmapBlockID extends BlockID {
	public IbmapBlockID(int mapNum) {
		super(Constants.ibmapUuidHigh, mapNum, Ibmap.name(mapNum) ,BlockType.IbmapBlock);
	}

	@Override
	public Block newBlock() {
		return new Ibmap((int)lowBits);
	}
}
