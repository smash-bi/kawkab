package kawkab.fs.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import kawkab.fs.commons.Commons;
import kawkab.fs.commons.Constants;

public class Cache {
	private static Cache instance;
	private ArrayList<Ibmap> ibmaps; //FIXME: This is temporary. We need to get maps from persistent storage.
	private Map<String, DataBlock> dataBlocks;
	private Map<Integer, InodesBlock> inodesBlocks;
	
	private Cache(){}
	
	public static Cache instance(){
		if (instance == null) {
			instance = new Cache();
		}
		
		return instance;
	}
	
	public InodesBlock getInodesBlock(int blockNumber){
		InodesBlock blk = inodesBlocks.get(blockNumber);
		
		if (blk == null){
			blk = InodesBlock.bootstrap(blockNumber);
			inodesBlocks.put(blockNumber, blk);
		}
		
		return blk;
	}
	
	Ibmap getIbmap(int blockIndex){
		return ibmaps.get(blockIndex);
	}
	
	DataBlock getDataBlock(long uuidHigh, long uuidLow){
		String uuid = Commons.uuidToString(uuidHigh, uuidLow);
		DataBlock block = dataBlocks.get(uuid);
		/*if (block == null){
			block = new DataBlock(uuidHigh, uuidLow);
			dataBlocks.put(uuid, block);
		}*/
		
		return block;
	}
	
	DataBlock newDataBlock(){
		UUID uuid = UUID.randomUUID();
		String uuidStr = Commons.uuidToString(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
		DataBlock block = new DataBlock(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
		dataBlocks.put(uuidStr, block);
		//System.out.println("\t\t\t Created data block " + uuidStr);
		
		return block;
	}
	
	void bootstrap(){
		ibmaps = new ArrayList<Ibmap>();
		for(int i=0; i<Constants.ibmapBlocksPerMachine; i++){
			ibmaps.add(Ibmap.bootstrap(i));
		}
		inodesBlocks = new HashMap<Integer, InodesBlock>();
		dataBlocks = new HashMap<String, DataBlock>();
	}
}
