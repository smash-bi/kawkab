package kawkab.fs.core;

import java.io.File;

import kawkab.fs.commons.Commons;
import kawkab.fs.commons.Constants;
import kawkab.fs.core.Block.BlockType;

/**
 * This class is supposed to be an immutable class.
 *
 */
public class DataSegmentID extends BlockID {
	public final int segmentInBlock; //Zero based segment index
	
	public DataSegmentID(long inumber, long blockInFile, int segmentInBlock) {
		super(inumber, blockInFile, name(inumber, blockInFile, segmentInBlock), BlockType.DataBlock);
		this.segmentInBlock = segmentInBlock;
	}
	
	@Override
	public int primaryNodeID() {
		return Commons.primaryWriterID(highBits);
	}

	@Override
	public Block newBlock() {
		return new DataSegment(this);
	}
	
	@Override
	public boolean equals(Object blockID){
		if (blockID == null)
			return false;
		
		DataSegmentID id;
		try {
			id = (DataSegmentID) blockID;
		}catch(Exception e){
			return false;
		}
		
		return highBits == id.highBits &&
				lowBits == id.lowBits &&
				segmentInBlock == id.segmentInBlock &&
				key.equals(id.key) &&
				type == id.type;
	}
	
	public static String name(long inumber, long blockInFile, int segmentInBlock) {
		return String.format("%016x-%016x-%08x", inumber, blockInFile, segmentInBlock);
	}

	@Override
	public String name() {
		return name(highBits, lowBits, segmentInBlock);
	}

	@Override
	public String localPath() {
		//TODO: May be a better approach is to use Base64 encoding for the inumber and use hex values
		//for the block number! Currently it is implemented as the encoding of both values.
		
		//String uuid = String.format("%016x%016x", id.highBits, id.lowBits);
		
		String uuid = Commons.uuidToString(highBits, lowBits); //highBits=inumber, lowBits=BlockNumber
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
	public String toString() {
		return highBits+"-"+lowBits+"-"+segmentInBlock+"-"+name();
	}

}
