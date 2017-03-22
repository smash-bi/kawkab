package kawkab.fs.core;

import kawkab.fs.api.FileHandle;
import kawkab.fs.api.FileOptions;
import kawkab.fs.commons.Constants;
import kawkab.fs.core.exceptions.IbmapsFullException;

public class Filesystem {
	private Cache cache;
	private static boolean initialized;
	public enum FileMode { READ, APPEND }
	
	private static Filesystem instance;
	private Namespace directory;
	
	private Filesystem(){
		directory = Namespace.instance();
		cache = Cache.instance();
		bootstrap();
	}
	
	public static Filesystem instance(){
		if (instance == null) {
			instance = new Filesystem();
		}
		return instance;
	}
	
	public FileHandle open(String filename, FileMode mode, FileOptions opts) throws IbmapsFullException{
		//TODO: Validate input
		long inumber = directory.openFile(filename);
		FileHandle file = new FileHandle(inumber, mode);
		return file;
	}
	
	public static int BlockSize(){
		return Constants.dataBlockSizeBytes;
	}
	
	void bootstrap(){
		if (!initialized){
			cache.bootstrap();
			initialized = true;
		}
	}
}
