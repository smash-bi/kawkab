package kawkab.fs.core;

import kawkab.fs.core.Block.BlockType;

/**
 * This class is supposed to be an immutable class.
 *
 */
public class DataSegmentID extends BlockID {
	public final int segmentInBlock; //Zero based segment index
	
	public DataSegmentID(long inumber, long blockInFile, int segmentInBlock) {
		super(inumber, blockInFile, DataSegment.name(inumber, blockInFile, segmentInBlock), BlockType.DataBlock);
		this.segmentInBlock = segmentInBlock;
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

}
