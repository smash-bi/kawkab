package kawkab.fs.core;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import kawkab.fs.commons.Constants;
import kawkab.fs.core.exceptions.IbmapsFullException;
import kawkab.fs.persistence.Cache;
import kawkab.fs.persistence.Ibmap;

public class Namespace {
	private Map<String, Long> filesMap;
	private Cache cache;
	private int lastIbmapUsed;
	private static Namespace instance;
	
	private Namespace(){
		filesMap = new ConcurrentHashMap<String, Long>();
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
		
		int mapNum = lastIbmapUsed;
		Ibmap ibmap = null;
		while(true){
			ibmap = cache.getIbmap(mapNum);
			inumber = ibmap.nextInode();
			if (inumber >=0)
				break;
			
			mapNum = (mapNum + 1) % Constants.ibmapBlocksPerMachine;
			if (mapNum == lastIbmapUsed){
				throw new IbmapsFullException();
			}
		}
		
		filesMap.put(filename, inumber);
		
		lastIbmapUsed = mapNum;
		return inumber;
	}
}
