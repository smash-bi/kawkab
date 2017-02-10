package kawkab.fs.api;

import java.util.HashMap;

import kawkab.fs.client.FileHandle;

public class Filesystem {
	private static Filesystem instance;
	private HashMap<Long, FileHandle> handles;
	
	private Filesystem(){}
	
	public static Filesystem instance(){
		if (instance == null) {
			instance = new Filesystem();
		}
		
		return instance;
	}
	
	public FileHandle create(FilePath path, FileOptions opts){
		//TODO: Validate input
		//TODO: Check if file already exists
		
		FileHandle file = new FileHandle(path, opts);
		
		//Save the new file handles
		//handles.put(path.uuid().getLeastSignificantBits(), file);
		
		return file;
	}
	
	public FileHandle open(FilePath path){
		return null;
	}
}
