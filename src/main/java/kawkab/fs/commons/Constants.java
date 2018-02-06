package kawkab.fs.commons;

import java.util.HashMap;
import java.util.Map;

import kawkab.fs.core.Inode;
import kawkab.fs.core.NodeInfo;
import kawkab.fs.core.zookeeper.ZKClusterConfig;

public class Constants {
	//TODO: Read constants and properties from a given configuration file.
	
	static {
		init();
	}
	
	public static int thisNodeID; //FIXME: Get this from a configuration file or command line. Node IDs start with 0.
	public static int nodesInSystem = 2; //FIXME: Get this from a configuration file or ZooKeeper
	
	public static final long maxBlocksPerLocalDevice = 1000;
	
	//Default data block size in bytes
	public static final int dataBlockSizeBytes    = 16*1024*1024;
	public static final int segmentsPerBlock      = 16;
	public static final int segmentSizeBytes = dataBlockSizeBytes/segmentsPerBlock;
	public static final int directBlocksPerInode = 0;
	//public static final int numPointersInIndexBlock = blockSegmentSizeBytes/IndexBlock.pointerSizeBytes;
	
	//Ibmap blocks range for this machine
	public static final int ibmapBlockSizeBytes = 16*1024; //FIXME: using a small number for testing
	public static final int ibmapsPerMachine = 1; //FIXME: Calculate this based on the maximum number of files supported per machine
	public static int ibmapBlocksRangeStart = thisNodeID*ibmapsPerMachine; //TODO: Get these numbers from a configuration file or ZooKeeper

	//Small inodesBlockSize shows better writes performance, perhaps due to locks in the InodesBlocks. The down side
	//is that small size files consume more disk resources.
	public static final int inodesBlockSizeBytes = 5120; //dataBlockSizeBytes; 
	public static final int inodeSizeBytes = Inode.inodesSize();
	public static final int inodesPerBlock = inodesBlockSizeBytes/inodeSizeBytes;
	//FIXME: Calculate this based on the maximum number of files supported by a machine
	public static final int inodeBlocksPerMachine = (int)(1.0/inodesPerBlock*ibmapBlockSizeBytes*ibmapsPerMachine*8.0);//inodesPerMachine/inodesPerBlock 
	public static int inodeBlocksRangeStart = thisNodeID*inodeBlocksPerMachine; //TODO: Get these numbers from a configuration file or ZooKeeper
																					 //Blocks start with ID 0.
	
	public static int maxBlocksInCache = 100; //Size of the cache in number of blocks
	public static int globalFetchExpiryTimeoutMs = 2500; //Expire data fetched from the global store after dataExpiryTimeoutMs
	public static int primaryFetchExpiryTimeoutMs = 500; //Expire data fetched from the primary node after primaryFetchExpiryTimeoutMs

	public static final int syncThreadsPerDevice = 2;
	public static final int numGlobalStoreLoadWorkers = 1;
	public static final int numGlobalStoreStoreWorkers = 1;
	
	public static final String basePath = "fs";
	public static final String ibmapsPath = basePath+"/ibmaps";
	public static final String inodeBlocksPath = basePath+"/inodes";
	public static final String blocksPath = basePath+"/blocks";
	public static final String namespacePath = basePath+"/namespace";
	public static final int inodeBlocksPerDirectory = 1000; //Number of inodesBlocks per directory in the local storage
	
	public static final long ibmapUuidHigh = 1; //High bits of uuid for ibmap. The low bits are the global blockIndex.
	public static final long inodesBlocksUuidHigh = 2;  //High bits of uuid for inodesBlock. The low bits are the global blockIndex.
	
	//ZooKeeper cluster settings
	public static final int zkMainClusterID = 1;
	public static final String zkMainServers = "10.0.1.100:2181,10.0.1.100:2182,10.0.1.100:2183";
	public static final int connectRetrySleepMs = 1000;
	public static final int connectMaxRetries = 5;
	public static final ZKClusterConfig zkMainCluster = 
			new ZKClusterConfig(zkMainClusterID, zkMainServers, connectRetrySleepMs, connectMaxRetries);
	
	
	//minio settings
	public static final String[] minioServers = {"http://10.0.1.100:9000"};
	public static final String minioAccessKey = "kawkab"; //Length must be at least 5 characters long. This should match minio server settings.
	public static final String minioSecretKey = "kawkabsecret"; //Length must be at least 8 characters long. This should match minio server settings.
	
	//gRPC service
	public static final int primaryNodeServicePort = 22332;
	
	public static Map<Integer, NodeInfo> nodesMap;
	
	static {
		verify();
	}
	
	public static void printConfig(){
		System.out.println(String.format("This node ID ............. = %d",thisNodeID));
		System.out.println(String.format("Nodes in system .......... = %d",nodesInSystem));
		System.out.println(String.format("Data block size MB ....... = %.3f",dataBlockSizeBytes/1024.0/1024.0));
		System.out.println(String.format("Data block segment size MB = %.3f",segmentSizeBytes/1024.0/1024.0));
		System.out.println(String.format("Num. of segments per block = %d", segmentsPerBlock));
		System.out.println(String.format("Direct blocks per inode .. = %d", directBlocksPerInode));
		//System.out.println(String.format("Pointers per index block . = %d", numPointersInIndexBlock));
		System.out.println(String.format("Maximum file size MB ..... = %.3f", Inode.maxFileSize/1024.0/1024.0));
		System.out.println();
		System.out.println(String.format("Ibmap block size MB ...... = %.3f", ibmapBlockSizeBytes/1024.0/1024.0));
		System.out.println(String.format("Ibmap blocks per machine . = %d", ibmapsPerMachine));
		System.out.println(String.format("Ibmaps total size MB ..... = %.3f", ibmapBlockSizeBytes*ibmapsPerMachine/1024.0/1024.0));
		System.out.println(String.format("Ibmaps range start ....... = %d", ibmapBlocksRangeStart));
		System.out.println();
		System.out.println(String.format("Inode block size MB ...... = %.3f", inodesBlockSizeBytes/1024.0/1024.0));
		System.out.println(String.format("Inode size bytes ......... = %d", inodeSizeBytes));
		System.out.println(String.format("Inodes per block ......... = %d", inodesPerBlock));
		System.out.println(String.format("Inode blocks per machine . = %d", inodeBlocksPerMachine));
		System.out.println(String.format("Inode blocks total size MB = %.3f", inodeBlocksPerMachine*inodesBlockSizeBytes/1024.0/1024.0));
		System.out.println(String.format("Inode blocks range start . = %d", inodeBlocksRangeStart));
	}
	
	private static void init() {
		String nodeID = System.getenv("kawkabNodeID");
		if (nodeID == null) {
			thisNodeID = 1;
		} else {
			thisNodeID = Integer.parseInt(nodeID);
		}
		
		nodesMap = new HashMap<Integer, NodeInfo>();
		nodesMap.put(1, new NodeInfo(1, "10.0.1.100"));
		nodesMap.put(2, new NodeInfo(1, "10.0.7.3"));
	}
	
	private static void verify() {
		assert thisNodeID >= 0;
		
		assert inodesBlockSizeBytes % inodesPerBlock == 0;
		assert dataBlockSizeBytes == segmentsPerBlock*segmentSizeBytes;
		
		assert ibmapBlockSizeBytes <= segmentSizeBytes;
		assert inodesBlockSizeBytes <= segmentSizeBytes;
		
		assert minioAccessKey.length() >= 5; //From minio documentation
		assert minioSecretKey.length() >= 8; //From minio documentation
	}
}
