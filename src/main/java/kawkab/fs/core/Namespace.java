package kawkab.fs.core;

import java.io.IOException;

import kawkab.fs.commons.Constants;
import kawkab.fs.core.exceptions.FileAlreadyExistsException;
import kawkab.fs.core.exceptions.FileNotExistException;
import kawkab.fs.core.exceptions.IbmapsFullException;
import kawkab.fs.core.exceptions.KawkabException;
import kawkab.fs.core.zookeeper.NamespaceService;

public class Namespace {
	//private PersistentMap filesMap;
	private NamespaceService ns;
	private Cache cache;
	private int lastIbmapUsed;
	private KeyedLock locks;
	
	private static Namespace instance;
	
	
	private Namespace() throws KawkabException {
		cache = Cache.instance();
		locks = new KeyedLock();
		ns = NamespaceService.instance();
	}
	
	public static Namespace instance() throws KawkabException {
		if (instance == null){
			instance = new Namespace();
		}
		return instance;
	}
	
	/**
	 * @param filename
	 * @return inumber of the file
	 * @throws IbmapsFullException if all the ibmaps are full for this node, which means the
	 *          filesystem has reached the maximum number of files limit.
	 * @throws IOException 
	 * @throws KawkabException 
	 */
	public long openFile(String filename) throws IbmapsFullException, IOException, KawkabException {
		Long inumber = -1L;
		
		//Lock namespace for the given filename
		locks.lock(filename);
		
		try {
			//inumber = filesMap.get(filename);
			try {
				inumber = ns.getInumber(filename);
			} catch(FileNotExistException e) {     //If the file does not exist
				inumber = null;
			}
			
			if (inumber == null) {                 //if the file does not exist, create a new file
				inumber = createNewFile();
				//filesMap.put(filename, inumber);
				
				try {
					ns.addFile(filename, inumber);
				} catch (FileAlreadyExistsException e) { // If the file already exists, e.g., because another 
					                                     // node created the same file with different inumber
					releaseInumber(inumber);
					
					inumber = ns.getInumber(filename); // We may get another exception if another node deletes 
					                                   // the file immediately after creating the file. In that case,
					                                   // the exception will be passed to the caller.
				} 
				   
			}
			
			//TODO: update openFilesTable
			
		} finally {
			locks.unlock(filename);
		}
		
		return inumber;
	}
	
	private void releaseInumber(long inumber) {
		//TODO: Need to implement this function
	}
	
	/**
	 * Allocates a new Inumber
	 * This must be called with the lock on the file that is being created.
	 * @return returns the inumber of the new file.
	 * @throws IbmapsFullException
	 * @throws IOException 
	 */
	private long getNewInumber() throws IbmapsFullException, IOException {
		long inumber;
		int mapNum = lastIbmapUsed;
		while(true){ //Iterate over the ibmap blocks.
			//try(Ibmap ibmap = cache.getIbmap(mapNum)) {
			BlockID id = new IbmapBlockID(mapNum);
			Ibmap ibmap = null;
			
			try {
				ibmap = (Ibmap)(cache.acquireBlock(id));
				inumber = ibmap.nextInode();
				if (inumber >=0)
					break;
				
				mapNum = (mapNum + 1) % Constants.ibmapBlocksPerMachine;
				if (mapNum == lastIbmapUsed){
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
	 * Creates a new file: allocates a new inumber, initializes the inode, and updates the openFilesTable.
	 * 
	 * This function must be called with the lock on the file that is being created.
	 * @return the inumber of the new file
	 * @throws IbmapsFullException
	 * @throws IOException 
	 */
	private long createNewFile() throws IbmapsFullException, IOException{
		long inumber = getNewInumber();
		
		int blockIndex = InodesBlock.blockIndexFromInumber(inumber);
		BlockID id = new InodesBlockID(blockIndex);
		InodesBlock inodes = null;
		try {
			inodes = (InodesBlock)cache.acquireBlock(id);
			inodes.initInode(inumber);
		} finally {
			if (inodes != null) {
				cache.releaseBlock(inodes.id());
			}
		}
		
		return inumber;
	}
	
	void bootstrap() throws IOException { //We don't need to bootstrap if we are using a distributed namesapce through ZooKeeper
		//filesMap = PersistentMap.instance();
		//System.out.println("Already existing files = " + filesMap.size());
		//ns = NamespaceService.instance();
	}
	
	static void shutdown(){
		//TODO: Stop new requests
	}
}
