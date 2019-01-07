package kawkab.fs.core;

import java.io.File;

import kawkab.fs.commons.Commons;
import kawkab.fs.commons.Constants;

public final class IbmapBlockID extends BlockID {
	private final static String namePrefix = "M";
	private final int mapNum;
	
	public IbmapBlockID(int mapNum) {
		super(name(mapNum) ,BlockType.IBMAP_BLOCK);
		this.mapNum = mapNum;
	}
	
	@Override
	public int primaryNodeID() {
		return Commons.ibmapOwner(mapNum);
	}
	
	@Override
	public Block newBlock() {
		return new Ibmap(this);
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
	public int perBlockKey() {
		return mapNum;
	}
	
	@Override
	public String toString() {
		return name(mapNum);
	}

	public int blockIndex() {
		return mapNum;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + mapNum;
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
		IbmapBlockID other = (IbmapBlockID) obj;
		if (mapNum != other.mapNum)
			return false;
		return true;
	}
	
	
}
