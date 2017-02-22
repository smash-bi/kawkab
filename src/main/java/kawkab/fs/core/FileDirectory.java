package kawkab.fs.core;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class FileDirectory {
	private Map<String, FileIndex> filesMap;
	
	public FileDirectory(){
		filesMap = new ConcurrentHashMap<String, FileIndex>();
	}
	
	public FileIndex add(String filename){
		FileIndex metadata = new FileIndex(filesMap.size(), filename);
		filesMap.put(filename, metadata);
		
		return metadata;
	}
	
	public FileIndex get(String filename){
		return filesMap.get(filename);
	}
}
