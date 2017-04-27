package kawkab.fs.core;

import kawkab.fs.core.Block.BlockType;

public class BlockID {
	//All the public fields in this class must be made immutable.
	public final long uuidHigh;
	public final long uuidLow;
	public final String key;
	public final BlockType type;
	
	//FIXME: Chnage the variable names from uuidHigh/Low to idHigh/Low because some blocks do not
	//have proper uuid as the id, e.g., ibmap and inodesBlocks.
	
	public BlockID(long uuidHigh, long uuidLow, String key, BlockType type){
		this.uuidHigh = uuidHigh;
		this.uuidLow = uuidLow;
		this.key = key;
		this.type = type;
	}
	
	public BlockID(BlockID id) {
		this.uuidHigh = id.uuidHigh;
		this.uuidLow = id.uuidLow;
		this.key = id.key;
		this.type = id.type;
	}
	
	@Override
	public String toString(){
		return uuidHigh+"-"+uuidLow+"-"+key;
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
		
		return uuidHigh == id.uuidHigh &&
				uuidLow == id.uuidLow &&
				key.equals(id.key) &&
				type == id.type;
	}
}
