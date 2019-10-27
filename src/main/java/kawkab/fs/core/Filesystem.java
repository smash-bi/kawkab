package kawkab.fs.core;

import kawkab.fs.api.FileOptions;
import kawkab.fs.commons.Configuration;
import kawkab.fs.core.exceptions.*;
import kawkab.fs.core.services.grpc.PrimaryNodeServiceServer;

import java.io.IOException;
import java.util.Properties;

public final class Filesystem {
	private static volatile boolean initialized;
	private static volatile boolean closed;
	
	public enum FileMode { READ, APPEND }
	
	private static Filesystem instance;
	private static Namespace namespace;
	private static PrimaryNodeServiceServer ns;
	private static Configuration conf;
	//private static FFilesystemServiceServer fs;

	private Filesystem() throws KawkabException, IOException {
		namespace = Namespace.instance();
		ns = new PrimaryNodeServiceServer();
		//fs = new FFilesystemServiceServer(this);
		ns.startServer();
		//fs.startServer();
	}
	
	public static synchronized Filesystem instance() throws KawkabException, IOException {
		if (instance == null) {
			throw new KawkabException("Filesystem is not bootstrapped. First call Fileysystem.bootstrap(nodeID, config)");
		}
		
		return instance;
	}
	
	/**
	 * This function opens a file for reading and appending. If the file is opened in the append mode and the file 
	 * doesn't exist, a new file is created. This node becomes the primary writer of a newly created file.
	 * 
	 * @param filename
	 * @param mode Append mode allows to read and append the file. Read mode allows only to read the file.
	 * @param opts Currently its just a place holder
	 * @return A FileHandle that can be used to perform read, append, close, and delete operations on the file.
	 * 
	 * @throws IbmapsFullException if the system is full and no new file can be created unless an existing file is deleted
	 * @throws IOException
	 * @throws FileNotExistException If the file is opened in the read mode and the file does not exist in the system
	 * @throws FileAlreadyOpenedException If the file is opened in the append mode more than once
	 * @throws KawkabException
	 * @throws InterruptedException
	 */
	public FileHandle open(String filename, FileMode mode, FileOptions opts) 
			throws IbmapsFullException, IOException, FileAlreadyOpenedException, FileNotExistException, KawkabException, InterruptedException {
		//TODO: Validate input
		
		assert opts.recordSize() > 0;
		assert opts.recordSize() <= Configuration.instance().segmentSizeBytes;
		
		long inumber = namespace.openFile(filename, mode == FileMode.APPEND, opts);
		System.out.println("[FS] Opened file: " + filename + ", inumber: " + inumber);
		FileHandle file = new FileHandle(inumber, mode, opts);
		return file;
	}

	public void close(FileHandle fh) throws KawkabException {
		System.out.println("[FS] Closing file: " + fh.inumber());
		fh.close();
		if (fh.mode() == FileMode.APPEND)
			namespace.closeAppendFile(fh.inumber());
	}
	
	/**
	 * This function bootstraps the filesystem on this node. It should be called once when the system is started. It
	 * bootstraps the namespace, the ibmaps, and the inodeblocks.
	 * 
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws KawkabException
	 * @throws AlreadyConfiguredException 
	 */
	public static synchronized Filesystem bootstrap(int nodeID, Properties confProps) throws IOException, InterruptedException, KawkabException, AlreadyConfiguredException {
		if (initialized) {
			System.out.println("\tFilesystem is already bootstraped, not doing it again...");
			return instance;
		}
		
		Configuration.configure(nodeID, confProps);
		
		instance = new Filesystem();

		
		InodesBlock.bootstrap();
		Ibmap.bootstrap();
		namespace.bootstrap();
		
		
		
		initialized = true;
		
		return instance;
	}
	
	public synchronized void shutdown() throws KawkabException, InterruptedException{
		if (closed)
			return;
		
		closed = true;
		//fs.stopServer();
		//ns.stopServer();
		namespace.shutdown();
		//Ibmap.shutdown();
		//	InodesBlock.shutdown();
		Inode.shutdown(); //FIXME: There should not be any inode shutdown method.
		Cache.instance().shutdown();
		Clock.instance().shutdown();
		
		synchronized(instance) {
			instance.notifyAll();
		}
		
		System.out.println("Closed FileSystem");
		// GlobalStoreManager.instance().shutdown(); //The LocalStore closes the GlobalStore
	}
	
	public boolean initialized() {
		return initialized;
	}
	
	public void waitUntilShutdown() throws InterruptedException {
		synchronized(instance) {
			instance.wait();
		}
	}
	
	public Configuration getConf() {
		return Configuration.instance();
	}
}
