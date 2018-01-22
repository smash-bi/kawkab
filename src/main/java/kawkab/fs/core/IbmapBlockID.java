package kawkab.fs.core;

import java.io.File;

import kawkab.fs.commons.Commons;
import kawkab.fs.commons.Constants;
import kawkab.fs.core.Block.BlockType;

public class IbmapBlockID extends BlockID {
	public IbmapBlockID(int mapNum) {
		super(Constants.ibmapUuidHigh, mapNum, name(mapNum) ,BlockType.IbmapBlock);
	}
	
	@Override
	public int primaryNodeID() {
		return Commons.ibmapOwner(lowBits);
	}
	
	@Override
	public Block newBlock() {
		return new Ibmap((int)lowBits);
	}

	public static String name(int blockIndex) {
		return "ibmap"+blockIndex;
	}
	
	@Override
	public String name() {
		return name((int)lowBits);
	}

	@Override
	public String localPath() {
		return Constants.ibmapsPath +File.separator+ name((int)lowBits);
	}
}
