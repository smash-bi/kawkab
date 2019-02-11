package kawkab.fs.commons;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import kawkab.fs.core.NodeInfo;
import kawkab.fs.core.exceptions.AlreadyConfiguredException;
import kawkab.fs.core.zookeeper.ZKClusterConfig;

public final class Configuration {
	private static Configuration instance;
	
	public final int thisNodeID; //FIXME: Get this from a configuration file or command line. Node IDs start with 0.
	
	//Default data block size in bytes
	public final int dataBlockSizeBytes; // = 64*1024*1024;
	public final int segmentSizeBytes; // = 1*1024*1024;
	public final int segmentsPerBlock; // = dataBlockSizeBytes/segmentSizeBytes;
	public final int directBlocksPerInode = 0;
	
	public final long maxFileSizeBytes = Long.MAX_VALUE;
	//public final int numPointersInIndexBlock = blockSegmentSizeBytes/IndexBlock.pointerSizeBytes;
	
	//Ibmap blocks range for this machine
	public final int ibmapBlockSizeBytes; // = 1*1024; //FIXME: using a small number for testing
	public final int ibmapsPerMachine; // = 1; //FIXME: Calculate this based on the maximum number of files supported per machine
	public final int ibmapBlocksRangeStart; // = thisNodeID*ibmapsPerMachine; //TODO: Get these numbers from a configuration file or ZooKeeper

	//Small inodesBlockSize shows better writes performance, perhaps due to locks in the InodesBlocks. The down side
	//is that small size files consume more disk resources.
	public final int inodesBlockSizeBytes; // = 1*1000; //dataBlockSizeBytes; 
	public final int inodeSizeBytes; // = 50;//Inode.inodesSize();
	public final int inodesPerBlock; // = inodesBlockSizeBytes/inodeSizeBytes;
	//FIXME: Calculate this based on the maximum number of files supported by a machine
	public final int inodeBlocksPerMachine; // = (int)((8.0d*ibmapBlockSizeBytes*ibmapsPerMachine)/inodesPerBlock);//inodesPerMachine/inodesPerBlock 
	public final int inodeBlocksRangeStart; // = thisNodeID*inodeBlocksPerMachine; //TODO: Get these numbers from a configuration file or ZooKeeper
																					 //Blocks start with ID 0.
	
	public final int maxBlocksPerLocalDevice; // = 20510 + inodeBlocksPerMachine + ibmapsPerMachine; //FIXME: Should it not be a long value???
	public final int maxBlocksInCache; //        = 20000; //Size of the cache in number of blocks. The blocks are ibmaps, inodeBlocks, and data segments (not data blocks)
	
	public final int dataSegmentFetchExpiryTimeoutMs; //  = 10000; //Expire data fetched from the global store after dataExpiryTimeoutMs
	public final int inodesBlockFetchExpiryTimeoutMs; //  = 2000; //Expire data fetched from the global store after dataExpiryTimeoutMs
	//public final int primaryFetchExpiryTimeoutMs = 5000; //Expire data fetched from the primary node after primaryFetchExpiryTimeoutMs

	public final int syncThreadsPerDevice; // = 1;
	public final int numWorkersStoreToGlobal; // = 8;
	//public final int numWorkersLoadFromGlobal = 5;
	
	public final int grpcClientFrameSize; // = dataBlockSizeBytes > 4194304 ? dataBlockSizeBytes+2048 : 4194304; //Frame size to use when fetching blocks from primary nodes, at least 4MB
	
	public final String basePath; // = "fs";
	public final String ibmapsPath; // = basePath+"/ibmaps";
	public final String inodeBlocksPath; // = basePath+"/inodes";
	public final String blocksPath; // = basePath+"/blocks";
	public final String namespacePath; // = basePath+"/namespace";
	public final int inodeBlocksPerDirectory; // = 1000; //Number of inodesBlocks per directory in the local storage
	
	public final long ibmapUuidHigh = 1; //High bits of uuid for ibmap. The low bits are the global blockIndex.
	public final long inodesBlocksUuidHigh = 2;  //High bits of uuid for inodesBlock. The low bits are the global blockIndex.
	
	//ZooKeeper cluster settings
	public final int zkMainClusterID; // = 1;
	public final String zkMainServers; // = "10.10.0.6:2181,10.10.0.6:2182,10.10.0.6:2183";
	public final int connectRetrySleepMs = 1000;
	public final int connectMaxRetries = 5;
	public final ZKClusterConfig zkMainCluster;
	
	public final int recordSize = 1; //Temporary record size until we implement structured files
	
	
	//minio settings
	public final String[] minioServers; // = {"http://10.10.0.6:9000"};
	public final String minioAccessKey; // = "kawkab"; //Length must be at least 5 characters long. This should match minio server settings.
	public final String minioSecretKey; // = "kawkabsecret"; //Length must be at least 8 characters long. This should match minio server settings.
	
	//gRPC service
	public final int primaryNodeServicePort; // = 22332;

	// Filesystem RPC service for the filesystem clients
	public final int fsServerListenPort; // = 33433;
	public final int maxBufferLen; // = 16*1024; //in bytes
	
	public final Map<Integer, NodeInfo> nodesMap;
	
	public static Configuration instance() {
		//assert instance != null;
		
		return instance;
	}
	
	public static Configuration configure(int nodeID, Properties props) throws AlreadyConfiguredException {
		if (instance != null)
			throw new AlreadyConfiguredException();
		
		instance = new Configuration(nodeID, props);
		return instance;
	}
	
	private Configuration(int nodeID, Properties props) {
		thisNodeID 				= nodeID;
		
		dataBlockSizeBytes		= Integer.parseInt(props.getProperty("dataBlockSizeBytes", "67108864").trim()); // 64MB
		segmentSizeBytes		= Integer.parseInt(props.getProperty("segmentSizeBytes", "1048576").trim()); // 1MB

		ibmapBlockSizeBytes		= Integer.parseInt(props.getProperty("ibmapBlockSizeBytes", "1024").trim());
		ibmapsPerMachine		= Integer.parseInt(props.getProperty("ibmapsPerMachine", "1").trim());

		inodesBlockSizeBytes	= Integer.parseInt(props.getProperty("inodesBlockSizeBytes", "1000").trim()); 
		inodeSizeBytes			= Integer.parseInt(props.getProperty("inodeSizeBytes", "50").trim());
			
		maxBlocksPerLocalDevice	= Integer.parseInt(props.getProperty("maxBlocksPerLocalDevice", "30000").trim());
		maxBlocksInCache		= Integer.parseInt(props.getProperty("maxBlocksInCache", "20000").trim());
			
		dataSegmentFetchExpiryTimeoutMs  = Integer.parseInt(props.getProperty("dataSegmentFetchExpiryTimeoutMs", "10000").trim());
		inodesBlockFetchExpiryTimeoutMs  = Integer.parseInt(props.getProperty("inodesBlockFetchExpiryTimeoutMs", "2000").trim());

		syncThreadsPerDevice	= Integer.parseInt(props.getProperty("syncThreadsPerDevice", "1").trim());
		numWorkersStoreToGlobal	= Integer.parseInt(props.getProperty("numWorkersStoreToGlobal", "4").trim());
			
		// Folders in the underlying filesystem
		basePath		= props.getProperty("basePath", "fs");
		inodeBlocksPerDirectory = Integer.parseInt(props.getProperty("inodeBlocksPerDirectory", "1000").trim());

		// ZooKeeper cluster settings
		zkMainClusterID	= Integer.parseInt(props.getProperty("zkMainClusterID", "1").trim());
		zkMainServers	= props.getProperty("zkMainServers", "10.10.0.6:2181,10.10.0.6:2182,10.10.0.6:2183");
			
		// minio settings (S3 emulator)
		minioServers	= props.getProperty("minioServers", "http://10.10.0.6:9000").split(",");
		minioAccessKey	= props.getProperty("minioAccessKey", "kawkab"); // Length must be at least 5 characters long. This should match minio server settings.
		minioSecretKey	= props.getProperty("minioSecretKey", "kawkabsecret"); // Length must be at least 8 characters long. This should match minio server settings.
			
		// gRPC RPC server between the nodes to read data from the primary nodes
		primaryNodeServicePort = Integer.parseInt(props.getProperty("primaryNodeServicePort", "22332").trim());

		// RPC service for the filesystem clients
		fsServerListenPort	= Integer.parseInt(props.getProperty("fsServerListenPort", "33433").trim());
		maxBufferLen		= Integer.parseInt(props.getProperty("maxBufferLen", "16384").trim()); // in bytes
		
		Map<Integer, NodeInfo> map = new HashMap<Integer, NodeInfo>();
		int nodesCount = Integer.parseInt(props.getProperty("nodesCount", "1").trim());
		for (int i=0; i<nodesCount; i++) {
			map.put(i, new NodeInfo(i, props.getProperty("node."+i, "10.10.0.7").trim()));
		}
		
		nodesMap = map;
		
		segmentsPerBlock = dataBlockSizeBytes/segmentSizeBytes;
		ibmapBlocksRangeStart = thisNodeID*ibmapsPerMachine; //TODO: Get these numbers from a configuration file or ZooKeeper
		inodesPerBlock = inodesBlockSizeBytes/inodeSizeBytes;
		
		inodeBlocksPerMachine = (int)((8.0d*ibmapBlockSizeBytes*ibmapsPerMachine)/inodesPerBlock); 
		inodeBlocksRangeStart = thisNodeID*inodeBlocksPerMachine; //TODO: Get these numbers from a configuration file or ZooKeeper
		grpcClientFrameSize = dataBlockSizeBytes > 4194304 ? dataBlockSizeBytes+2048 : 4194304; // At least 4MB
		
		ibmapsPath = basePath+"/ibmaps";
		inodeBlocksPath = basePath+"/inodes";
		blocksPath = basePath+"/blocks";
		namespacePath = basePath+"/namespace";
		
		//ZooKeeper cluster settings
		zkMainCluster =	new ZKClusterConfig(zkMainClusterID, zkMainServers, connectRetrySleepMs, connectMaxRetries);
		
		//GCMonitor.initialize();
		
		printConfig();
		
		verify();
		
	}
	
	public void printConfig(){
		System.out.println(String.format("This node ID ............. = %d",thisNodeID));
		System.out.println(String.format("Data block size MB ....... = %.3f",dataBlockSizeBytes/1024.0/1024.0));
		System.out.println(String.format("Data block segment size MB = %.3f",segmentSizeBytes/1024.0/1024.0));
		System.out.println(String.format("Num. of segments per block = %d", segmentsPerBlock));
		System.out.println(String.format("Direct blocks per inode .. = %d", directBlocksPerInode));
		//System.out.println(String.format("Pointers per index block . = %d", numPointersInIndexBlock));
		System.out.println(String.format("Maximum file size MB ..... = %.3f", maxFileSizeBytes/1024.0/1024.0));
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
		System.out.println(String.format("Max blocks per local device= %d", maxBlocksPerLocalDevice));
		System.out.println();
		System.out.println(String.format("Max blocks in cache ...... = %d", maxBlocksInCache));
	}
	
	private void verify() {
		assert thisNodeID >= 0;
		
		assert inodesBlockSizeBytes % inodesPerBlock == 0;
		assert dataBlockSizeBytes == segmentsPerBlock*segmentSizeBytes;
		
		assert ibmapBlockSizeBytes <= segmentSizeBytes;
		assert inodesBlockSizeBytes <= segmentSizeBytes;
		
		assert minioAccessKey.length() >= 5; //From minio documentation
		assert minioSecretKey.length() >= 8; //From minio documentation
		
		assert maxBlocksPerLocalDevice > inodeBlocksPerMachine + ibmapsPerMachine;
		//assert maxBlocksPerLocalDevice > maxBlocksInCache;
	}
}
