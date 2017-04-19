package kawkab.fs.core;

import java.io.File;
import java.io.IOException;

import kawkab.fs.commons.Constants;
import kawkab.fs.core.Block.BlockType;
import kawkab.fs.core.exceptions.IbmapsFullException;
import net.openhft.chronicle.map.ChronicleMap;

public class Namespace {
	private ChronicleMap<String, Long> filesMap;
	private Cache cache;
	private int lastIbmapUsed;
	private KeyedLock locks;
	
	private static Namespace instance;
	
	
	private Namespace(){
		cache = Cache.instance();
		locks = new KeyedLock();
	}
	
	public static Namespace instance(){
		if (instance == null){
			instance = new Namespace();
		}
		return instance;
	}
	
	/**
	 * @param filename
	 * @return 
	 * @throws IbmapsFullException if all the ibmaps are full for this node, which means the
	 *          filesystem has reached the maximum number of files limit.
	 * @throws IOException 
	 */
	public long openFile(String filename) throws IbmapsFullException, IOException {
		Long inumber = -1L;
		
		//Lock namespace for the given filename
		locks.lock(filename);
		
		try {
			inumber = filesMap.get(filename);
			
			//if the file does not exist, create a new file
			if (inumber == null) {
				inumber = createNewFile();
				filesMap.put(filename, inumber);
			}
			
			//TODO: update openFilesTable
			
		} finally {
			locks.unlock(filename);
		}
		
		return inumber;
	}
	
	/**
	 * Allocates a new Inumber
	 * This must be called with the lock on the file that is being created.
	 * @return returns the inumber of the new file.
	 * @throws IbmapsFullException
	 */
	private long getNewInumber() throws IbmapsFullException{
		long inumber;
		int mapNum = lastIbmapUsed;
		while(true){ //Iterate over the ibmap blocks.
			//try(Ibmap ibmap = cache.getIbmap(mapNum)) {
			BlockID id = new BlockID(Constants.ibmapUuidHigh, mapNum, Ibmap.name(mapNum), BlockType.IbmapBlock);
			try(Ibmap ibmap = (Ibmap)(cache.acquireBlock(id))) {
				inumber = ibmap.nextInode();
				if (inumber >=0)
					break;
				
				mapNum = (mapNum + 1) % Constants.ibmapBlocksPerMachine;
				if (mapNum == lastIbmapUsed){
					throw new IbmapsFullException();
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
	 */
	private long createNewFile() throws IbmapsFullException{
		long inumber = getNewInumber();
		
		int blockIndex = InodesBlock.blockIndexFromInumber(inumber);
		BlockID id = new BlockID(Constants.inodesBlocksUuidHigh, blockIndex, InodesBlock.name(blockIndex), BlockType.InodeBlock);
		try (InodesBlock inodes = (InodesBlock)cache.acquireBlock(id)){
			inodes.initInode(inumber);
		}
		
		return inumber;
	}
	
	void bootstrap() throws IOException {
		String path = Constants.namespacePath + "/" + "kawkab-namespace";
		File file = new File(Constants.namespacePath);
		if (!file.exists()){
			file.mkdirs();
		}
		
		filesMap = ChronicleMap
				.of(String.class, Long.class)
			    .name("Kawkab-namespace")
			    .averageKeySize(32)
			    .entries(1000000)
			    .createPersistedTo(new File(path));
		
		System.out.println("Already existing files = " + filesMap.size());
	}
	
	static void shutdown(){
		//TODO: Stop new requests
	}
}
