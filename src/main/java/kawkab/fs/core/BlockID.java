package kawkab.fs.core;

import kawkab.fs.core.Block.BlockType;

public class BlockID {
	//All the public fields in this class must be made immutable.
	public final long highBits;
	public final long lowBits;
	public final String key;
	public final BlockType type;
	
	//FIXME: Chnage the variable names from uuidHigh/Low to idHigh/Low because some blocks do not
	//have proper uuid as the id, e.g., ibmap and inodesBlocks.
	
	public BlockID(long highBits, long lowBits, String key, BlockType type){
		this.highBits = highBits;
		this.lowBits = lowBits;
		this.key = key;
		this.type = type;
	}
	
	public BlockID(BlockID id) {
		this.highBits = id.highBits;
		this.lowBits = id.lowBits;
		this.key = id.key;
		this.type = id.type;
	}
	
	@Override
	public String toString(){
		//return highBits+"-"+lowBits+"-"+key;
		return key;
	}
	
	@Override
	public boolean equals(Object blockID){
		System.out.println("===== Block ID equals method ======");
		
		if (blockID == null)
			return false;
		
		BlockID id;
		try {
			id = (BlockID) blockID;
		}catch(Exception e){
			return false;
		}
		
		System.out.println("Comparing " + this + " and " + id);
		
		return highBits == id.highBits &&
				lowBits == id.lowBits &&
				key.equals(id.key) &&
				type == id.type;
	}

	public boolean isValid() {
		return highBits != 0 && lowBits != 0;
	}
	
	public Block newBlock() {
		//TODO: Use an alternate solution where BlockIDs are of different types and they
		//create only one type of blocks.
		Block block;
		if (type == BlockType.IbmapBlock) {
			block = new Ibmap((int)lowBits);
		} else if (type == BlockType.InodeBlock) {
			block = new InodesBlock((int)lowBits);
		} else if (type == BlockType.DataBlock) {
			block = new DataBlock(this);
		} else {
			throw new IllegalArgumentException("Block type is invalid: " + type);
		}
		
		return block;
	}
}
