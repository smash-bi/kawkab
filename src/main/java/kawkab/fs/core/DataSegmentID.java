package kawkab.fs.core;

import java.io.File;

import kawkab.fs.commons.Commons;
import kawkab.fs.commons.Constants;

/**
 * This class is supposed to be an immutable class.
 *
 */
public final class DataSegmentID extends BlockID {
	private final long inumber;
	private final long blockInFile;
	private final int segmentInBlock; //Zero based segment index
	
	/**
	 * @param inumber inode number of the file
	 * @param blockInFile block number in the file
	 * @param segmentInBlock segment number in the block
	 * @param loadFromPrimary Loading from the primary node is enabled or not
	 */
	public DataSegmentID(long inumber, long blockInFile, int segmentInBlock) {
		super(name(inumber, blockInFile, segmentInBlock), BlockType.DATA_SEGMENT);
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
	
	public static String name(long inumber, long blockInFile, int segmentInBlock) {
		return String.format("DS%d-%d-%d", inumber, blockInFile, segmentInBlock);
	}

	@Override
	public String name() {
		return name(inumber, blockInFile, segmentInBlock);
	}

	@Override
	public String localPath() {
		//TODO: May be a better approach is to use Base64 encoding for the inumber and use hex values
		//for the block number! Currently it is implemented as the encoding of both values.
		
		//String uuid = String.format("%016x%016x", id.highBits, id.lowBits);
		
		String uuid = Commons.uuidToString(inumber, blockInFile); //highBits=inumber, lowBits=BlockNumber
		int uuidLen = uuid.length();
		
		int wordSize = 3; //Number of characters of the Base64 encoding that make a directory
		int levels = 6; //Number of directory levels //FIXME: we will have 64^3 files in a directory, 
						//which is 262144. This may slow down the underlying filesystem.
		
		assert wordSize * levels < uuidLen-1;
		
		StringBuilder path = new StringBuilder(Constants.blocksPath.length()+uuidLen+levels);
		path.append(Constants.blocksPath + File.separator);
		int rootLen = uuidLen - levels*wordSize;
		path.append(uuid.substring(0, rootLen));
		
		for (int i=0; i<levels; i++){
			path.append(File.separator).append(uuid.substring(rootLen+i*wordSize, rootLen+i*wordSize+wordSize));
		}
		//path.append(File.separator).append(uuid.substring(levels*wordSize));
		
		return path.toString();
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
		return name();
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + (int) (blockInFile ^ (blockInFile >>> 32));
		result = prime * result + (int) (inumber ^ (inumber >>> 32));
		result = prime * result + segmentInBlock;
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
		DataSegmentID other = (DataSegmentID) obj;
		if (blockInFile != other.blockInFile)
			return false;
		if (inumber != other.inumber)
			return false;
		if (segmentInBlock != other.segmentInBlock)
			return false;
		return true;
	}
}
