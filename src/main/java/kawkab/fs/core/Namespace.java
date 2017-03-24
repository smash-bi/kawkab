package kawkab.fs.core;

import java.io.File;
import java.io.IOException;

import kawkab.fs.commons.Constants;
import kawkab.fs.core.exceptions.IbmapsFullException;
import net.openhft.chronicle.map.ChronicleMap;

public class Namespace {
	private ChronicleMap<String, Long> filesMap;
	private Cache cache;
	private int lastIbmapUsed;
	private static Namespace instance;
	
	private Namespace(){
		cache = Cache.instance();
	}
	
	public static Namespace instance(){
		if (instance == null){
			instance = new Namespace();
		}
		return instance;
	}
	
	/**
	 * @param filename
	 * @return 
	 * @throws IbmapsFullException if all the ibmaps are full for this node, which means the
	 *          filesystem has reached the maximum number of files limit.
	 */
	public long openFile(String filename) throws IbmapsFullException{
		Long inumber = filesMap.get(filename);
		if (inumber != null) {
			return inumber;
		}
		
		inumber = createNewFile();
		
		filesMap.put(filename, inumber);
		
		
		return inumber;
	}
	
	private long createNewFile() throws IbmapsFullException{
		long inumber;
		int mapNum = lastIbmapUsed;
		while(true){
			try(Ibmap ibmap = cache.getIbmap(mapNum)) {
				inumber = ibmap.nextInode();
				if (inumber >=0)
					break;
				
				mapNum = (mapNum + 1) % Constants.ibmapBlocksPerMachine;
				if (mapNum == lastIbmapUsed){
					throw new IbmapsFullException();
				}
			}
		}
		
		lastIbmapUsed = mapNum;
		
		return inumber;
	}
	
	void bootstrap() throws IOException {
		String path = Constants.namespacePath + "/" + "kawkab-namespace";
		File file = new File(Constants.namespacePath);
		if (!file.exists()){
			file.mkdirs();
		}
		
		filesMap = ChronicleMap
				.of(String.class, Long.class)
			    .name("Kawkab-namespace")
			    .averageKeySize(32)
			    .entries(1000000)
			    .createPersistedTo(new File(path));
		
		System.out.println("Already existing files = " + filesMap.size());
	}
	
	static void shutdown(){
		//TODO: Stop new requests
	}
}
