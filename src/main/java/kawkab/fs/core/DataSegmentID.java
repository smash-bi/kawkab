package kawkab.fs.core;

import java.io.File;
import java.util.Objects;

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
	private final int recordSize;
	private String localPath;
	private int hash;
	private int hash2;
	private static int numDevices = Configuration.instance().numLocalDevices;
	private static final String blocksPath = Configuration.instance().blocksPath;
	
	/**
	 * @param inumber inode number of the file
	 * @param blockInFile block number in the file
	 * @param segmentInBlock segment number in the block
	 */
	public DataSegmentID(long inumber, long blockInFile, int segmentInBlock, int recordSize) {
		//TODO: Remove recordSize as that is not part of the ID

		super(BlockType.DATA_SEGMENT);
		this.inumber = inumber;
		this.blockInFile = blockInFile;
		this.segmentInBlock = segmentInBlock;
		this.recordSize = recordSize;
	}

	public int recordSize() {
		return recordSize;
	}
	
	@Override
	public int primaryNodeID() {
		return Commons.primaryWriterID(inumber);
	}

	@Override
	public Block newBlock() {
		return new DataSegment(this);
	}

	@Override
	public String localPath() {
		//TODO: May be a better approach is to use Base64 encoding for the inumber and use hex values
		//for the block number! Currently it is implemented as the encoding of both values.
		
		//String uuid = String.format("%016x%016x", id.highBits, id.lowBits);
		
		if (localPath != null)
			return localPath;
		
		String uuid = Commons.uuidToBase64String(inumber, blockInFile);
		int uuidLen = uuid.length();
		int wordSize = 3; //Number of characters of the Base64 encoding that make a directory
		int levels = 6; //Number of directory levels //FIXME: we will have 64^3 files in a directory, 
						//which is 262144. This may slow down the underlying filesystem.
		
		assert wordSize * levels < uuidLen-1;

		StringBuilder path = new StringBuilder(blocksPath.length()+uuidLen+levels+4); //4 for extra head-room
		path.append("fs").append(Math.abs(perBlockTypeKey()) % numDevices).append(File.separator).append(blocksPath).append(File.separator);
		
		int rootLen = uuidLen - levels*wordSize; // The root folder can have more characters than wordSize
		path.append(uuid, 0, rootLen);
		
		for (int i=0; i<levels; i++){
			path.append(File.separator).append(uuid, rootLen+i*wordSize, rootLen+i*wordSize+wordSize);
		}
		
		localPath = path.toString().intern(); //TODO: Should we intern() this string?

		return localPath;
	}

	@Override
	public int perBlockTypeKey() {
		if (hash2 == 0)
			hash2 = Objects.hash(inumber, blockInFile);
		return hash2;
	}

	@Override
	public String fileID() {
		return "D"+inumber+blockInFile;
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
	
	@Override
	public int hashCode() {
		if (hash == 0)
			hash = Objects.hash(inumber, blockInFile, segmentInBlock);
		return hash;
		//return Objects.hash(inumber, blockInFile, segmentInBlock);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		
		//>> if this.hasCode == that.hashCode, return true << // https://norvig.com/java-iaq.html
		
		DataSegmentID that = (DataSegmentID) o;
		return inumber == that.inumber &&
				blockInFile == that.blockInFile &&
				segmentInBlock == that.segmentInBlock;
	}
}

