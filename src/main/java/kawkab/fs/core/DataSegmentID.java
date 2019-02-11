package kawkab.fs.core;

import java.io.File;

import kawkab.fs.commons.Commons;
import kawkab.fs.commons.Configuration;

/**
 * This class is supposed to be an immutable class.
 *
 */
public final class DataSegmentID extends BlockID {
	private final long inumber;
	private final long blockInFile;
	private final int segmentInBlock; //Zero based segment index
	private String localPath;
	private int hash;
	
	private static final String blocksPath = Configuration.instance().blocksPath;
	
	/**
	 * @param inumber inode number of the file
	 * @param blockInFile block number in the file
	 * @param segmentInBlock segment number in the block
	 * @param loadFromPrimary Loading from the primary node is enabled or not
	 */
	public DataSegmentID(long inumber, long blockInFile, int segmentInBlock) {
		super(BlockType.DATA_SEGMENT);
		this.inumber = inumber;
		this.blockInFile = blockInFile;
		this.segmentInBlock = segmentInBlock;
	}
	
	@Override
	public int primaryNodeID() {
		return Commons.primaryWriterID(inumber);
	}

	@Override
	public Block newBlock() {
		return new DataSegment(this);
	}
	
	/*public static String name(long inumber, long blockInFile, int segmentInBlock) {
		return "D"+inumber+"-"+blockInFile+"-"+segmentInBlock;
		//return String.format("DS%d-%d-%d", inumber, blockInFile, segmentInBlock);
	}*/

	/*@Override
	public String name() {
		return this.uniqueKey();
	}*/

	@Override
	public String localPath() {
		//TODO: May be a better approach is to use Base64 encoding for the inumber and use hex values
		//for the block number! Currently it is implemented as the encoding of both values.
		
		//String uuid = String.format("%016x%016x", id.highBits, id.lowBits);
		
		if (localPath != null)
			return localPath;
		
		String uuid = Commons.uuidToBase64String(inumber, blockInFile); //highBits=inumber, lowBits=BlockNumber
		int uuidLen = uuid.length();
		
		int wordSize = 3; //Number of characters of the Base64 encoding that make a directory
		int levels = 6; //Number of directory levels //FIXME: we will have 64^3 files in a directory, 
						//which is 262144. This may slow down the underlying filesystem.
		
		assert wordSize * levels < uuidLen-1;
		
		StringBuilder path = new StringBuilder(blocksPath.length()+uuidLen+levels);
		path.append(blocksPath + File.separator);
		
		int rootLen = uuidLen - levels*wordSize;
		path.append(uuid.substring(0, rootLen));
		
		for (int i=0; i<levels; i++){
			path.append(File.separator).append(uuid.substring(rootLen+i*wordSize, rootLen+i*wordSize+wordSize));
		}
		
		localPath = path.toString();
		
		return localPath;
	}
	
	@Override
	public int perBlockKey() {
		return (int)((inumber<<16) | blockInFile); //FIXME: Use a hash function such as Murmur3_32
	}
	
	public long inumber() {
		return inumber;
	}
	
	public long blockInFile() {
		return blockInFile;
	}
	
	public int segmentInBlock() {
		return segmentInBlock;
	}
	
	@Override
	public String toString() {
		return "D-"+inumber+"-"+blockInFile+"-"+segmentInBlock;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		if (hash != 0)
			return hash;
		
		final int prime = 13; // [SR] random prime
		int result = 17; // [SR] random
		result = prime * result + (int) (blockInFile ^ (blockInFile >>> 32));
		result = prime * result + (int) (inumber ^ (inumber >>> 32));
		result = prime * result + segmentInBlock;
		result = prime * result + ((type == null) ? 0 : (type.ordinal()*7)); // [SR] 7 is a random prime
		
		hash = result;
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
		DataSegmentID other = (DataSegmentID) obj;
		if (blockInFile != other.blockInFile)
			return false;
		if (inumber != other.inumber)
			return false;
		if (segmentInBlock != other.segmentInBlock)
			return false;
		if (type != other.type)
			return false;
		return true;
	}

	
}
