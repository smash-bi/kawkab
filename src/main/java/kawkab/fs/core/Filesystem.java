package kawkab.fs.core;

import java.io.IOException;

import kawkab.fs.api.FileHandle;
import kawkab.fs.api.FileOptions;
import kawkab.fs.commons.Constants;
import kawkab.fs.core.exceptions.IbmapsFullException;

public class Filesystem {
	private static boolean initialized;
	private static boolean closed;
	
	public enum FileMode { READ, APPEND }
	
	private static Filesystem instance;
	private Namespace namespace;
	
	private Filesystem(){
		namespace = Namespace.instance();
	}
	
	public static Filesystem instance(){
		if (instance == null) {
			instance = new Filesystem();
		}
		return instance;
	}
	
	public FileHandle open(String filename, FileMode mode, FileOptions opts) throws IbmapsFullException{
		//TODO: Validate input
		long inumber = namespace.openFile(filename);
		FileHandle file = new FileHandle(inumber, mode);
		return file;
	}
	
	public static int BlockSize(){
		return Constants.dataBlockSizeBytes;
	}
	
	public Filesystem bootstrap() throws IOException{
		if (initialized)
			return this;
		
		InodesBlock.bootstrap();
		Ibmap.bootstrap();
		namespace.bootstrap();
		
		initialized = true;
		return this;
	}
	
	public static void shutdown(){
		if (closed)
			return;
		
		Namespace.shutdown();
		Ibmap.shutdown();
		InodesBlock.shutdown();
		
		closed = true;
	}
}
