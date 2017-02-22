package kawkab.fs.core;

import kawkab.fs.api.FileHandle;
import kawkab.fs.api.FileOptions;

public class Filesystem {
	public enum FileMode {
		READ, APPEND
	}
	
	private static Filesystem instance;
	private FileDirectory directory;
	
	private Filesystem(){
		directory = new FileDirectory();
	}
	
	public static Filesystem instance(){
		if (instance == null) {
			instance = new Filesystem();
		}
		
		return instance;
	}
	
	public FileHandle open(String filename, FileMode mode, FileOptions opts){
		//TODO: Validate input
		//TODO: Check if file already exists
		
		FileIndex fileIndex = directory.get(filename);
		if (fileIndex == null){
			fileIndex = directory.add(filename);
		}
		
		FileHandle file = new FileHandle(filename, opts, fileIndex);
		
		//Save file handles
		
		return file;
	}
}
