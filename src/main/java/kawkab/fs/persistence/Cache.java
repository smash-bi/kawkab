package kawkab.fs.persistence;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import kawkab.fs.commons.Constants;
import kawkab.fs.core.BlockMetadata;
import kawkab.fs.core.FileIndex;
import kawkab.fs.core.IndexBlock;

public class Cache {
	private static Cache instance;
	private ArrayList<Ibmap> ibmaps; //FIXME: This is temporary. We need to get maps from persistent storage.
	private Map<String, BlockMetadata> dataBlocks;
	
	private Cache(){
		ibmaps = new ArrayList<Ibmap>();
		for(int i=0; i<Constants.ibmapBlocksPerMachine; i++){
			ibmaps.add(new Ibmap(i));
		}
		
		dataBlocks = new HashMap<String, BlockMetadata>();
	}
	
	public static Cache instance(){
		if (instance == null) {
			instance = new Cache();
		}
		
		return instance;
	}
	
	public FileIndex getInode(long uuidHigh, long uuidLow){
		return null;
	}
	
	public Ibmap getIbmap(int blockIndex){
		return ibmaps.get(blockIndex);
	}
	
	public BlockMetadata getDataBlock(long uuidHigh, long uuidLow){
		String uuid = stringFromUUID(uuidHigh, uuidLow);
		BlockMetadata block = dataBlocks.get(uuid);
		if (block == null){
			block = new BlockMetadata(uuidHigh, uuidLow);
			dataBlocks.put(uuid, block);
		}
		
		return block;
	}
	
	private String stringFromUUID(long uuidHigh, long uuidLow){
		//TODO: This just a stub! convert UUID in base64 encoding
		return uuidHigh+""+uuidLow; 
	}
	
	public BlockMetadata newDataBlock(){
		UUID uuid = UUID.randomUUID();
		
		while (uuid.getMostSignificantBits() == 0 && uuid.getLeastSignificantBits() == 0){
			uuid = UUID.randomUUID();
		}
		
		BlockMetadata block = new BlockMetadata(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
		return block;
	}
}
