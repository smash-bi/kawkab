package kawkab.fs.core;

import java.io.File;

import kawkab.fs.commons.Commons;
import kawkab.fs.commons.Configuration;

public final class IbmapBlockID extends BlockID {
	private final int mapNum;
	private String localPath;
	private static final String ibmapsPath = Configuration.instance().ibmapsPath+File.separator;
	
	public IbmapBlockID(int mapNum) {
		super(BlockType.IBMAP_BLOCK);
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

	/*public static String name(int blockIndex) {
		return namePrefix+blockIndex;
	}*/
	
	/*@Override
	public String name() {
		return name(mapNum);
	}*/

	@Override
	public String localPath() {
		if (localPath == null)
			localPath = ibmapsPath+mapNum;
		
		return localPath;
	}
	
	@Override
	public int perBlockKey() {
		return mapNum;
	}
	
	@Override
	public String toString() {
		return "B"+mapNum;
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
		int result = 1;
		result = prime * result + mapNum;
		result = prime * result + ((type == null) ? 0 : (type.ordinal()*37)); //37 is just a random prime number
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
		IbmapBlockID other = (IbmapBlockID) obj;
		if (mapNum != other.mapNum)
			return false;
		if (type != other.type)
			return false;
		return true;
	}

	
	
}
