package kawkab.fs.core;

import java.io.File;
import java.io.IOException;

import kawkab.fs.commons.Constants;
import net.openhft.chronicle.map.ChronicleMap;

public final class LocalStoreDB {
	private static final String mapFilePath = Constants.basePath + "/localStoreDB/chroniclemap-"+Constants.thisNodeID;
	private static final String mapName = "localStoreDB";
	private ChronicleMap<String, String> map; // Path to ID map. A key is a block's path because the data segments belonging
	                                          // to the same block have the same path. In this way, we save the number of
	                                          // entries in the map.
	
	public LocalStoreDB() throws IOException {
		initMap();
	}
	
	private void initMap() throws IOException {
		System.out.println("Map file path: " + mapFilePath);
		File file = new File(mapFilePath).getParentFile();
		if (!file.exists()) {
			file.mkdirs();
		}
		
		map = ChronicleMap
				.of(String.class, String.class)
			    .name(mapName)
			    .averageKeySize(72)
			    .averageValueSize(32)
			    .entries(1000000)
			    .createPersistedTo(new File(mapFilePath));
	}
	
	/**
	 * @return Previous location associated with blockName, or null if blockName was not already in the DB.
	 */
	public synchronized String put(BlockID id) {
		//System.out.println("[LSDB] Added: " + id.name());
		return map.put(id.localPath(), id.name());
	}
	
	/**
	 * @param blockName
	 * @return Location of the blockName, or null if the blockName does not exist in the DB.
	 *//*
	public synchronized String get(String blockName) {
		return map.get(blockName);
	}*/
	
	/**
	 * @param blockName
	 * @return Whether the blockName entry exists in the DB or not
	 */
	public synchronized boolean exists(BlockID id) {
		//System.out.println("[LSDB] Exists: " + id.name());
		return map.containsKey(id.localPath());
	}
	
	/**
	 * @param blockName
	 * @return Location of the blockName, or null if blockName entry was not in the DB.
	 */
	public synchronized String removeEntry(BlockID id) {
		//System.out.println("[LSDB] Removed: " + id.localPath());
		return map.remove(id.localPath());
	}
}