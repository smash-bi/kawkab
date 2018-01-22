package kawkab.fs.core;

import java.io.IOException;

import kawkab.fs.api.FileHandle;
import kawkab.fs.api.FileOptions;
import kawkab.fs.commons.Constants;
import kawkab.fs.core.exceptions.IbmapsFullException;
import kawkab.fs.core.exceptions.KawkabException;
import kawkab.fs.core.services.PrimaryNodeServiceServer;

public class Filesystem {
	private static boolean initialized;
	private static boolean closed;
	
	public enum FileMode { READ, APPEND }
	
	private static Filesystem instance;
	private Namespace namespace;
	private PrimaryNodeServiceServer ns;
	
	private Filesystem() throws KawkabException, IOException {
		namespace = Namespace.instance();
		ns = new PrimaryNodeServiceServer();
		ns.startServer();
	}
	
	public static synchronized Filesystem instance() throws KawkabException, IOException {
		if (instance == null) {
			instance = new Filesystem();
		}
		return instance;
	}
	
	public FileHandle open(String filename, FileMode mode, FileOptions opts) throws IbmapsFullException, IOException, KawkabException{
		//TODO: Validate input
		long inumber = namespace.openFile(filename, mode == FileMode.APPEND);
		System.out.println("[FS] Opened file: " + filename + ", inumber: " + inumber);
		FileHandle file = new FileHandle(inumber, mode);
		return file;
	}
	
	public static int BlockSize(){
		return Constants.segmentSizeBytes;
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
	
	public synchronized void shutdown() throws KawkabException, InterruptedException{
		if (closed)
			return;
		
		closed = true;
		ns.stopServer();
		namespace.shutdown();
		Ibmap.shutdown();
		InodesBlock.shutdown();
		Cache.instance().shutdown();
		
	}
}
