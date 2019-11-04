package kawkab.fs.commons;

import kawkab.fs.core.NodeInfo;
import kawkab.fs.core.exceptions.AlreadyConfiguredException;
import kawkab.fs.core.exceptions.KawkabException;
import kawkab.fs.core.zookeeper.ZKClusterConfig;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public final class Configuration {
	private static Configuration instance;
	
	public final static String propsFileLocal = "config-local.properties";
	public final static String propsFileCluster = "config.properties";
	
	public final int thisNodeID; //FIXME: Get this from a configuration file or command line. Node IDs start with 0.
	
	//Default data block size in bytes
	public final int dataBlockSizeBytes; // = 64*1024*1024;
	public final int segmentSizeBytes; // = 1*1024*1024;
	public final int segmentsPerBlock; // = dataBlockSizeBytes/segmentSizeBytes;
	public final int directBlocksPerInode = 0;
	
	public final long maxFileSizeBytes = Long.MAX_VALUE;

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
																					 //Blocks start with ID 0.public final int indexNodeSizeBytes; // Size of the indexBlock.

	// Constants related to PostOrderHeapIndex
	public final int percentIndexEntriesPerNode;
	public final int indexBlockSizeBytes; // Size of the indexBlock. Each block is a file in the local filesystem. A block is a collection of index nodes (POHNode).
	public final int indexNodeSizeBytes; // Size of the POHNode. Each node have index entries and index pointers.
	public final int nodesPerBlockPOH; // Number of POHNodes in each index block


	public final int maxBlocksPerLocalDevice; // = 20510 + inodeBlocksPerMachine + ibmapsPerMachine; //FIXME: Should it not be a long value???
	//public final int maxBlocksInCache; //        = 20000; //Size of the cache in number of blocks. The blocks are ibmaps, inodeBlocks, and data segments (not data blocks)
	public final int cacheSizeMiB;	// Size of the cache in MB
	
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
	public final String indexBlocksPath; // = basePath+"/inodes";
	public final String blocksPath; // = basePath+"/blocks";
	public final String namespacePath; // = basePath+"/namespace";
	public final int inodeBlocksPerDirectory = 10000; //Number of inodesBlocks per directory in the local storage
	public final int indexBlocksPerDirectory = 10000; //Number of indexBlocks per directory in the local storage
	
	//ZooKeeper cluster settings
	public final int zkMainClusterID; // = 1;
	public final String zkMainServers; // = "10.10.0.6:2181,10.10.0.6:2182,10.10.0.6:2183";
	public final int connectRetrySleepMs = 1000;
	public final int connectMaxRetries = 5;
	public final ZKClusterConfig zkMainCluster;
	
	//minio settings
	public final String[] minioServers; // = {"http://10.10.0.6:9000"};
	public final String minioAccessKey; // = "kawkab"; //Length must be at least 5 characters long. This should match minio server settings.
	public final String minioSecretKey; // = "kawkabsecret"; //Length must be at least 8 characters long. This should match minio server settings.
	
	//Thrift service port for remote reads
	public final int primaryNodeServicePort; // = 22332;

	// Filesystem RPC service for the filesystem clients
	public final int fsServerListenPort; // = 33433;
	public final int maxBufferLen; // = 16*1024; //in bytes
	
	public final Map<Integer, NodeInfo> nodesMap;
	
	public static Configuration instance() {
		assert instance != null;
		
		return instance;
	}
	
	public static Configuration configure(int nodeID, Properties props) throws AlreadyConfiguredException {
		if (instance != null)
			throw new AlreadyConfiguredException();
		
		instance = new Configuration(nodeID, props);
		return instance;
	}
	
	private Configuration(int nodeID, Properties props) {
		System.out.println("Configuring the system...");
		thisNodeID = nodeID;
		
		for(Object keyObj : props.keySet()) {
			String key = (String)keyObj;
			props.setProperty(key, props.getProperty(key).trim());
		}
		
		dataBlockSizeBytes		= Integer.parseInt(props.getProperty("dataBlockSizeBytes", "67108864")); // 64MB
		segmentSizeBytes		= Integer.parseInt(props.getProperty("segmentSizeBytes", "1048576")); // 1MB

		ibmapBlockSizeBytes		= Integer.parseInt(props.getProperty("ibmapBlockSizeBytes", "1024"));
		ibmapsPerMachine		= Integer.parseInt(props.getProperty("ibmapsPerMachine", "1"));

		inodesBlockSizeBytes	= Integer.parseInt(props.getProperty("inodesBlockSizeBytes", "1000")); 
		inodeSizeBytes			= Integer.parseInt(props.getProperty("inodeSizeBytes", "50"));
			
		maxBlocksPerLocalDevice	= Integer.parseInt(props.getProperty("maxBlocksPerLocalDevice", "30000"));
		//maxBlocksInCache		= Integer.parseInt(props.getProperty("maxBlocksInCache", "20000"));
		cacheSizeMiB				= Integer.parseInt(props.getProperty("cacheSizeMiB", "10000"));
			
		dataSegmentFetchExpiryTimeoutMs  = Integer.parseInt(props.getProperty("dataSegmentFetchExpiryTimeoutMs", "10000"));
		inodesBlockFetchExpiryTimeoutMs  = Integer.parseInt(props.getProperty("inodesBlockFetchExpiryTimeoutMs", "2000"));

		indexBlockSizeBytes = dataBlockSizeBytes;
		indexNodeSizeBytes = segmentSizeBytes; //Integer.parseInt(props.getProperty("indexBlockSizeBytes", ""+segmentSizeBytes));
		percentIndexEntriesPerNode = Integer.parseInt(props.getProperty("percentIndexEntriesPerNode", "70"));
		nodesPerBlockPOH = indexBlockSizeBytes/indexNodeSizeBytes;

		syncThreadsPerDevice	= Integer.parseInt(props.getProperty("syncThreadsPerDevice", "1"));
		numWorkersStoreToGlobal	= Integer.parseInt(props.getProperty("numWorkersStoreToGlobal", "4"));
			
		// Folders in the underlying filesystem
		basePath		= props.getProperty("basePath", "fs");
		//inodeBlocksPerDirectory = Integer.parseInt(props.getProperty("inodeBlocksPerDirectory", "1000"));

		// ZooKeeper cluster settings
		zkMainClusterID	= Integer.parseInt(props.getProperty("zkMainClusterID", "1"));
		zkMainServers	= props.getProperty("zkMainServers", "10.10.0.6:2181,10.10.0.6:2182,10.10.0.6:2183");
			
		// minio settings (S3 emulator)
		minioServers	= props.getProperty("minioServers", "http://10.10.0.6:9000").split(",");
		minioAccessKey	= props.getProperty("minioAccessKey", "kawkab"); // Length must be at least 5 characters long. This should match minio server settings.
		minioSecretKey	= props.getProperty("minioSecretKey", "kawkabsecret"); // Length must be at least 8 characters long. This should match minio server settings.
			
		// gRPC RPC server between the nodes to read data from the primary nodes
		primaryNodeServicePort = Integer.parseInt(props.getProperty("primaryNodeServicePort", "22332"));

		// RPC service for the filesystem clients
		fsServerListenPort	= Integer.parseInt(props.getProperty("fsServerListenPort", "33433"));
		maxBufferLen		= Integer.parseInt(props.getProperty("maxBufferLen", "16384")); // in bytes
		
		Map<Integer, NodeInfo> map = new HashMap<>();
		int nodesCount = Integer.parseInt(props.getProperty("nodesCount", "1"));
		for (int i=0; i<nodesCount; i++) {
			map.put(i, new NodeInfo(i, props.getProperty("node."+i, "10.10.0.7")));
		}
		
		nodesMap = map;
		
		segmentsPerBlock = dataBlockSizeBytes/segmentSizeBytes;
		ibmapBlocksRangeStart = thisNodeID*ibmapsPerMachine; //TODO: Get these numbers from a configuration file or ZooKeeper
		inodesPerBlock = inodesBlockSizeBytes/inodeSizeBytes;
		
		inodeBlocksPerMachine = (int)((8.0d*ibmapBlockSizeBytes*ibmapsPerMachine)/inodesPerBlock); 
		inodeBlocksRangeStart = thisNodeID*inodeBlocksPerMachine; //TODO: Get these numbers from a configuration file or ZooKeeper
		grpcClientFrameSize = dataBlockSizeBytes > 4194304 ? dataBlockSizeBytes+2048 : 4194304; // At least 4MB
		
		ibmapsPath = basePath+File.separator+"ibmaps";
		inodeBlocksPath = basePath+File.separator+"inodes";
		indexBlocksPath = basePath+File.separator+"index";
		blocksPath = basePath+File.separator+"blocks";
		namespacePath = basePath+File.separator+"namespace";
		
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
		System.out.println(String.format("Index node size bytes= %d", indexNodeSizeBytes));
		System.out.println(String.format("Index entries per block %% = %d", indexNodeSizeBytes));
		System.out.println(String.format("Cache size (MiB) ......... = %d", cacheSizeMiB));
	}
	
	private void verify() {
		assert thisNodeID >= 0;
		
		assert inodesBlockSizeBytes % inodesPerBlock == 0;
		assert dataBlockSizeBytes == segmentsPerBlock*segmentSizeBytes;
		
		assert ibmapBlockSizeBytes <= segmentSizeBytes;
		assert inodesBlockSizeBytes <= segmentSizeBytes;
		
		assert minioAccessKey.length() >= 5; //From minio documentation
		assert minioSecretKey.length() >= 8; //From minio documentation
		
		assert maxBlocksPerLocalDevice > inodeBlocksPerMachine + ibmapsPerMachine; //Inodes and Ibmaps are never evicted from the local storage
		
		assert cacheSizeMiB > (segmentSizeBytes/1048576.0);
		
		//assert indexBlockSizeBytes % 24 == 0; //For the time being, an index entry is 16 bytes, two timestamp, segInFile or indexNodeNumber
		
		assert powerOfTwo(segmentSizeBytes) : "segmentSizeBytes should be power of 2, currently it is: "+segmentSizeBytes;
		assert powerOfTwo(dataBlockSizeBytes) : "dataBlockSizeBytesshould be power of 2, currently it is: "+dataBlockSizeBytes;
		assert powerOfTwo(inodesBlockSizeBytes) : "inodesBlockSizeBytes should be power of 2, currently it is: "+inodesBlockSizeBytes;
		assert powerOfTwo(indexNodeSizeBytes) : "indexBlockSizeBytes should be power of 2, currently it is: "+ indexNodeSizeBytes;

		assert nodesPerBlockPOH > 0 : "nodesPerBlockPOH should be greater than zero, currently it is " + nodesPerBlockPOH;
	}
	
	private boolean powerOfTwo(long val){
		return val > 0 && ((val & (val-1)) == 0);
	}
	
	public static Properties getProperties(String propsFile) throws IOException {
		try (InputStream in = Commons.class.getClassLoader().getResourceAsStream(propsFile)) {
			assert in != null;
			
			Properties props = new Properties();
			props.load(in);
			
			return props;
		}
	}
	
	public static int getNodeID() throws KawkabException {
		String nodeIDProp = "nodeID";
		
		if (System.getProperty(nodeIDProp) == null) {
			throw new KawkabException("System property nodeID is not defined.");
		}
		
		return Integer.parseInt(System.getProperty(nodeIDProp));
	}
}
