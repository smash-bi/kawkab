package kawkab.fs.core;

import java.io.File;
import java.io.IOException;

import kawkab.fs.commons.Constants;
import net.openhft.chronicle.map.ChronicleMap;

public class PersistentMap {
	private static final Object initLock = new Object();
	
	private static PersistentMap instance;
	private ChronicleMap<String, Long> map;
	
	private PersistentMap() throws IOException{
		String path = Constants.namespacePath + "/" + "kawkab-kv";
		File file = new File(Constants.namespacePath);
		if (!file.exists()){
			file.mkdirs();
		}
		
		map = ChronicleMap
				.of(String.class, Long.class)
			    .name("Kawkab-KV")
			    .averageKeySize(32)
			    .entries(1000000)
			    .createPersistedTo(new File(path));
	}
	
	public static PersistentMap instance() throws IOException{
		if (instance == null) {
			synchronized(initLock) {
				if (instance == null)
					instance = new PersistentMap();
			}
		}
		
		return instance;
	}
	
	public void put(String key, long value){
		map.put(key, value);
	}
	
	public Long get(String key){
		return map.get(key);
	}
	
	public int size(){
		return map.size();
	}
}
