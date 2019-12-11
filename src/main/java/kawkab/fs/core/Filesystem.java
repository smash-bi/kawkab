package kawkab.fs.core;

import kawkab.fs.api.FileOptions;
import kawkab.fs.commons.Configuration;
import kawkab.fs.core.exceptions.*;
import kawkab.fs.core.services.thrift.FilesystemServiceServer;
import kawkab.fs.core.services.thrift.PrimaryNodeServiceServer;
import kawkab.fs.core.timerqueue.TimerQueue;
import kawkab.fs.core.timerqueue.TimerQueueIface;
import kawkab.fs.utils.GCMonitor;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public final class Filesystem {
	private static volatile boolean initialized;
	private static volatile boolean closed;

	public enum FileMode { READ, APPEND }
	
	private static Filesystem instance;
	private static Namespace namespace;
	private static Configuration conf;
	private static FilesystemServiceServer fss;
	private static PrimaryNodeServiceServer pns;
	private static Cache cache;

	private static Map<Long, FileHandle> openFiles; //FIXME: We should pass on a unique number to the FileHandle and use that as the key

	private TimerQueueIface fsQ;
	private TimerQueueIface segsQ;

	private Filesystem() throws KawkabException, IOException {
		namespace = Namespace.instance();
		pns = new PrimaryNodeServiceServer();
		pns.startServer();
		fss = new FilesystemServiceServer(this);
		fss.startServer();
		fsQ = new TimerQueue("FS Timer Queue");
		segsQ = new TimerQueue("Segs Timer Queue");
		openFiles = new HashMap<>();
		cache = Cache.instance();
		conf = Configuration.instance();
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
	public synchronized FileHandle open(String filename, FileMode mode, FileOptions opts)
			throws IbmapsFullException, IOException, FileAlreadyOpenedException, FileNotExistException, KawkabException, InterruptedException {
		//TODO: Validate input
		
		assert opts.recordSize() > 0;
		assert opts.recordSize() <= Configuration.instance().segmentSizeBytes;
		
		//long inumber = namespace.openFileDbg(filename, mode == FileMode.APPEND, opts);
		long inumber = namespace.openFile(filename, mode == FileMode.APPEND, opts);
		System.out.println("[FS] Opened file: " + filename + ", inumber: " + inumber);
		FileHandle file = new FileHandle(inumber, mode, fsQ, segsQ);
		verify(inumber, opts.recordSize());
		openFiles.put(file.inumber(), file);
		return file;
	}

	private void verify(long inumber, int recSize) throws IOException, KawkabException {
		BlockID id = new InodesBlockID((int) (inumber / conf.inodesPerBlock));
		InodesBlock inb = null;
		try {
			inb = (InodesBlock) cache.acquireBlock(id);
			inb.loadBlock();

			Inode inode = inb.getInode(inumber);
			if (inode.recordSize() != recSize) {
				throw new KawkabException(String.format("Record sizes do not match while opening the file %d. Given=%d, expected=%d",
						inumber, recSize, inode.recordSize()));
			}
		} finally {
			if (inb != null) {
				cache.releaseBlock(id);
			}
		}
	}

	public synchronized void close(FileHandle fh) throws KawkabException {
		System.out.println("[FS] Closing file: " + fh.inumber());
		fh.close();
		if (fh.mode() == FileMode.APPEND)
			namespace.closeAppendFile(fh.inumber());
	}

	public TimerQueueIface getTimerQueue() {
		return fsQ;
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
			System.out.println("\tFilesystem is already bootstrapped, not doing it again...");
			return instance;
		}

		System.out.println("Filesystem bootstrap...");
		
		Configuration.configure(nodeID, confProps);
		
		instance = new Filesystem();
		
		InodesBlock.bootstrap();
		Ibmap.bootstrap();
		namespace.bootstrap();
		GCMonitor.initialize();

		initialized = true;
		
		return instance;
	}
	
	public synchronized void shutdown() throws KawkabException, InterruptedException{
		if (closed)
			return;
		
		closed = true;
		fss.stopServer();
		pns.stopServer();
		namespace.shutdown();
		segsQ.shutdown();
		fsQ.shutdown();
		Cache.instance().shutdown();
		ApproximateClock.instance().shutdown();

		System.out.print("GC duration stats (ms): "); GCMonitor.printStats();
		System.out.println("Closed FileSystem");
		// GlobalStoreManager.instance().shutdown(); //The LocalStore closes the GlobalStore

		//Wakeup the threads waiting in waitUntilShutdown
		synchronized(instance) {
			instance.notifyAll();
		}
	}
	
	public boolean initialized() {
		return initialized;
	}
	
	public void waitUntilShutdown() throws InterruptedException {
		synchronized(instance) {
			instance.wait();
		}
	}

	public void flush() throws KawkabException {
		fsQ.waitUntilEmpty();
		segsQ.waitUntilEmpty();

		for(FileHandle fh : openFiles.values()) {
			try {
				fh.flush();
			} catch (FileHandleClosedException e) {
				//Ignore
			}
		}

		cache.flush();
	}
	
	public Configuration getConf() {
		return Configuration.instance();
	}

	public void printStats() throws KawkabException {
		for (FileHandle file : openFiles.values()) {
			file.printStats();
		}

		pns.printStats();
	}

	public void resetStats() throws KawkabException {
		for (FileHandle file : openFiles.values()) {
			file.resetStats();
		}

		pns.resetStats();
	}
}
