package kawkab.fs.core;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import kawkab.fs.commons.Commons;
import kawkab.fs.commons.Constants;
import kawkab.fs.core.exceptions.FileAlreadyExistsException;
import kawkab.fs.core.exceptions.FileNotExistException;
import kawkab.fs.core.exceptions.IbmapsFullException;
import kawkab.fs.core.exceptions.InvalidFileModeException;
import kawkab.fs.core.exceptions.KawkabException;
import kawkab.fs.core.zookeeper.NamespaceService;

public class Namespace {
	private static final Object initLock = new Object();

	// private PersistentMap filesMap;
	private NamespaceService ns;
	private Cache cache;
	private int lastIbmapUsed;
	private KeyedLock locks;

	private Map<Long, Boolean> openedFiles; // Map<inumber, appendMode>

	private static Namespace instance;

	private Namespace() throws KawkabException, IOException {
		cache = Cache.instance();
		locks = new KeyedLock();
		lastIbmapUsed = Constants.ibmapBlocksRangeStart;
		openedFiles = new ConcurrentHashMap<Long, Boolean>();
		ns = NamespaceService.instance();
	}

	public static Namespace instance() throws KawkabException, IOException {
		if (instance == null) {
			synchronized (initLock) {
				if (instance == null)
					instance = new Namespace();
			}
		}

		return instance;
	}

	/**
	 * @param filename
	 * @return inumber of the file
	 * @throws IbmapsFullException
	 *             if all the ibmaps are full for this node, which means the
	 *             filesystem has reached the maximum number of files limit.
	 * @throws IOException
	 * @throws KawkabException
	 * @throws InterruptedException
	 */
	public long openFile(String filename, boolean appendMode) throws IbmapsFullException, IOException,
			InvalidFileModeException, FileNotExistException, KawkabException, InterruptedException {
		long inumber = -1L;

		// Lock namespace for the given filename
		locks.lock(filename);

		try {
			// inumber = filesMap.get(filename);
			try {
				inumber = ns.getInumber(filename);
			} catch (FileNotExistException fnee) { // If the file does not exist
				if (appendMode) { // Create file if the file is opened in the append mode.
					// System.out.println("[NS] Creating new file: " + filename);
					inumber = createNewFile();

					try {
						ns.addFile(filename, inumber);

						// System.out.println("[N] Created a new file: " + filename + ", inumber: " +
						// inumber);
					} catch (FileAlreadyExistsException faee) { // If the file already exists, e.g., because another
																// node created the same file with different inumber
						releaseInumber(inumber);

						inumber = ns.getInumber(filename); // We may get another exception if another node deletes
						// the file immediately after creating the file. In that case,
						// the exception will be passed to the caller.
					}
				} else { // if the file cannot be created due to not being opened in the append mode.
					throw fnee;
				}
			}

			// TODO: update openFilesTable

			// if the file is opened in append mode and this node is not the primary file
			// writer
			if (appendMode && Commons.primaryWriterID(inumber) != Constants.thisNodeID) {
				throw new InvalidFileModeException(
						"Cannot open file in the append mode. Inumber of the file is out of range of this node's range.");
			}

			if (appendMode) {
				openAppendFile(inumber, filename);
			}
		} finally {
			locks.unlock(filename);
		}

		return inumber;
	}

	/**
	 * FIXME: This function is not finalized. This need to be updated to implement
	 * proper openFilesTable.
	 * 
	 * @param inumber
	 * @throws InvalidFileModeException
	 */
	private void openAppendFile(long inumber, String filename) throws InvalidFileModeException {
		Boolean alreadyOpened = openedFiles.put(inumber, true);

		if (alreadyOpened != null && alreadyOpened == true) {
			throw new InvalidFileModeException("File is already opened in the append mode; inumber=" + inumber+", File: "+filename);
		}
	}

	private void releaseInumber(long inumber) {
		// TODO: Need to implement this function
	}

	/**
	 * Allocates a new Inumber This must be called with the lock on the file that is
	 * being created.
	 * 
	 * @return returns the inumber of the new file.
	 * @throws IbmapsFullException
	 * @throws IOException
	 * @throws KawkabException
	 * @throws InterruptedException
	 */
	private long getNewInumber() throws IbmapsFullException, IOException, KawkabException, InterruptedException {
		long inumber;
		int mapNum = lastIbmapUsed;
		while (true) { // Iterate over the ibmap blocks.
			// try(Ibmap ibmap = cache.getIbmap(mapNum)) {

			BlockID id = new IbmapBlockID(mapNum);
			Ibmap ibmap = null;

			try {
				// System.out.println("[NS] Map number: " + mapNum);
				ibmap = (Ibmap) (cache.acquireBlock(id, false));
				inumber = ibmap.nextInode();
				if (inumber >= 0)
					break;

				mapNum = (mapNum + 1) % Constants.ibmapsPerMachine;
				if (mapNum == lastIbmapUsed) {
					throw new IbmapsFullException();
				}
			} finally {
				if (ibmap != null) {
					cache.releaseBlock(ibmap.id());
				}
			}
		}
		lastIbmapUsed = mapNum;

		return inumber;
	}

	/**
	 * Creates a new file: allocates a new inumber, initializes the inode, and
	 * updates the openFilesTable.
	 * 
	 * This function must be called with the lock on the file that is being created.
	 * 
	 * @return the inumber of the new file
	 * @throws IbmapsFullException
	 * @throws IOException
	 * @throws KawkabException
	 * @throws InterruptedException
	 */
	private long createNewFile() throws IbmapsFullException, IOException, KawkabException, InterruptedException {
		long inumber = getNewInumber();

		int blockIndex = InodesBlock.blockIndexFromInumber(inumber);
		BlockID id = new InodesBlockID(blockIndex);
		InodesBlock inodesBlock = null;
		try {
			// System.out.println("[NS] Inodes Block: " + blockIndex + ", inumber: " +
			// inumber + ", primary: " + Commons.primaryWriterID(inumber));

			inodesBlock = (InodesBlock) cache.acquireBlock(id, false);
			inodesBlock.initInode(inumber);
		} finally {
			if (inodesBlock != null) {
				cache.releaseBlock(inodesBlock.id());
			}
		}

		return inumber;
	}

	void bootstrap() throws IOException { // We don't need to bootstrap if we are using a distributed namesapce through
											// ZooKeeper
		// filesMap = PersistentMap.instance();
		// System.out.println("Already existing files = " + filesMap.size());
		// ns = NamespaceService.instance();
	}

	void shutdown() {
		// TODO: Stop new requests
	}
}
