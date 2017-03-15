package kawkab.fs.core;

import kawkab.fs.api.FileHandle;
import kawkab.fs.api.FileOptions;
import kawkab.fs.commons.Constants;
import kawkab.fs.core.exceptions.IbmapsFullException;

public class Filesystem {
	public enum FileMode {
		READ, APPEND
	}
	
	private static Filesystem instance;
	private Namespace directory;
	
	private Filesystem(){
		directory = Namespace.instance();
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
		
		FileHandle file = new FileHandle(filename, opts, inumber);
		
		//Save file handles
		
		return file;
	}
	
	public static int BlockSize(){
		return Constants.dataBlockSizeBytes;
	}
	
	private void bootstrap(){
		//TODO: Setup namespace, create ibmap and inode blocks
	}
}
