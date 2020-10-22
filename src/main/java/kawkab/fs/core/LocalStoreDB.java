package kawkab.fs.core;

import kawkab.fs.commons.Configuration;
import net.openhft.chronicle.map.ChronicleMap;

import java.io.File;
import java.io.IOException;

public final class LocalStoreDB implements LocalStoreDBIface{
	private final int id;
	private final int maxSize;
	private final String mapFilePath;
	private static final String mapName = "localStoreDB";
	private ChronicleMap<String, String> map; // Path to ID map. A key is a block's path because the data segments belonging
	                                          // to the same block have the same path. In this way, we save the number of
	                                          // entries in the map.

	public LocalStoreDB(int id, int maxSize) {
		this.id = id;
		this.maxSize = maxSize;
		mapFilePath = Configuration.instance().basePath + "/localStoreDB/chroniclemap-"+Configuration.instance().thisNodeID+"-"+id;
		initMap();
	}
	
	private void initMap() {
		System.out.println("Initializing ChronicleMap: " + mapFilePath);
		File file = new File(mapFilePath).getParentFile();
		if (!file.exists()) {
			file.mkdirs();
		}
		
		try {
			map = ChronicleMap
					.of(String.class, String.class)
				    .name(mapName)
				    .averageKeySize(32)
				    .averageValueSize(2)
				    .entries(maxSize+100)
				    .createPersistedTo(new File(mapFilePath));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public synchronized void put(BlockID id) {
		//System.out.println("[LSDB] Added: " + id.name());
		
		assert map.size()+1 <= maxSize;
		
		String prev = map.put(id.localPath(), "");
		
		if (prev != null) {
			System.out.println("\t Block already exists in the localstore: " + id + ", "+id.localPath());
		}
		
		assert prev == null;
		
		//System.out.println("Created block: "  + id +", "+ id.localPath());
	}
	
	/**
	 * @return Whether the blockName entry exists in the DB or not
	 */
	@Override
	public synchronized boolean exists(BlockID id) {
		//System.out.println("[LSDB] Exists: " + id.name());
		return map.containsKey(id.localPath());
	}
	
	/**
	 * @return Location of the blockName, or null if blockName entry was not in the DB.
	 */
	@Override
	public synchronized String removeEntry(BlockID id) {
		//System.out.println("[LSDB] Removed: " + id.localPath());
		return map.remove(id.localPath());
	}

	@Override
	public synchronized int size() {
		return map.size();
	}

	@Override
	public synchronized void shutdown() {
		map.close();
	}
}
