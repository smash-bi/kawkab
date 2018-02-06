package kawkab.fs.core;

import java.io.File;

import kawkab.fs.commons.Commons;
import kawkab.fs.commons.Constants;

public final class IbmapBlockID extends BlockID {
	private final static String namePrefix = "M";
	private final int mapNum;
	
	public IbmapBlockID(int mapNum) {
		super(name(mapNum) ,BlockType.IbmapBlock);
		this.mapNum = mapNum;
	}
	
	@Override
	public int primaryNodeID() {
		return Commons.ibmapOwner(mapNum);
	}
	
	@Override
	public Block newBlock() {
		return new Ibmap((int)mapNum);
	}

	public static String name(int blockIndex) {
		return namePrefix+blockIndex;
	}
	
	@Override
	public String name() {
		return name(mapNum);
	}

	@Override
	public String localPath() {
		return Constants.ibmapsPath +File.separator+ name(mapNum);
	}
	
	@Override
	public boolean areEqual(BlockID blockID){
		if (blockID == null)
			return false;
		
		if (!(blockID instanceof IbmapBlockID))
			return false;
		
		IbmapBlockID id;
		try {
			id = (IbmapBlockID) blockID;
		}catch(Exception e){
			return false;
		}
		
		return mapNum == id.mapNum;
	}
	
	@Override
	public String toString() {
		return name(mapNum);
	}
}
