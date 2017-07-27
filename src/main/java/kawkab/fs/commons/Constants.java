package kawkab.fs.commons;

import kawkab.fs.core.Inode;

public class Constants {
	public static final long maxBlocksPerLocalDevice = 1000;
	
	//Default data block size in bytes
	public static final int dataBlockSizeBytes    = 16*1024*1024;
	public static final int segmentsPerBlock      = 16;
	public static final int segmentSizeBytes = dataBlockSizeBytes/segmentsPerBlock;
	public static final int directBlocksPerInode = 0;
	//public static final int numPointersInIndexBlock = blockSegmentSizeBytes/IndexBlock.pointerSizeBytes;
	
	//Ibmap blocks range for this machine
	public static final int ibmapBlockSizeBytes = 16*1024; //FIXME: using a small number for testing
	public static final int ibmapBlocksPerMachine = 1; //FIXME: Calculate this based on the maximum number of files supported per machine
	public static int ibmapBlocksRangeStart = 0; //TODO: Get these numbers from a configuration file or ZooKeeper

	//Small inodesBlockSize shows better writes performance, perhaps due to locks in the InodesBlocks. The down side
	//is that small size files consume more disk resources.
	public static final int inodesBlockSizeBytes = 5120; //dataBlockSizeBytes; 
	public static final int inodeSizeBytes = Inode.inodesSize();
	public static final int inodesPerBlock = inodesBlockSizeBytes/inodeSizeBytes;
	public static final int inodeBlocksPerMachine = (int)Math.ceil(ibmapBlockSizeBytes*ibmapBlocksPerMachine*8.0/inodesPerBlock); //FIXME: Calculate this based on the maximum number of files supported by a machine
	public static int inodesBlocksRangeStart = 0; //TODO: Get these numbers from a configuration file or ZooKeeper
	
	public static int maxBlocksInCache = 100;
	public static final int syncThreadsPerDevice = 2;
	
	public static final String basePath = "fs";
	public static final String ibmapsPath = basePath+"/ibmaps";
	public static final String inodeBlocksPath = basePath+"/inodes";
	public static final String blocksPath = basePath+"/blocks";
	public static final String namespacePath = basePath+"/namespace";
	public static final int inodeBlocksPerDirectory = 1000;
	
	public static final long ibmapUuidHigh = 1; //High bits of uuid for ibmap. The low bits are the blockIndex
	public static final long inodesBlocksUuidHigh = 2;  //High bits of uuid for inodesBlock. The low bits are the blockIndex
	
	static {
		assert inodesBlockSizeBytes % inodesPerBlock == 0;
		assert dataBlockSizeBytes == segmentsPerBlock*segmentSizeBytes;
		
		assert ibmapBlockSizeBytes <= segmentSizeBytes;
		assert inodesBlockSizeBytes <= segmentSizeBytes;
	}
	
	public static void printConfig(){
		System.out.println(String.format("Data block size MB ....... = %.3f",dataBlockSizeBytes/1024.0/1024.0));
		System.out.println(String.format("Data block segment size MB = %.3f",segmentSizeBytes/1024.0/1024.0));
		System.out.println(String.format("Num. of segments per block = %d", segmentsPerBlock));
		System.out.println(String.format("Direct blocks per inode .. = %d", directBlocksPerInode));
		//System.out.println(String.format("Pointers per index block . = %d", numPointersInIndexBlock));
		System.out.println(String.format("Maximum file size MB ..... = %.3f", Inode.maxFileSize/1024.0/1024.0));
		System.out.println();
		System.out.println(String.format("Ibmap block size MB ...... = %.3f", ibmapBlockSizeBytes/1024.0/1024.0));
		System.out.println(String.format("Ibmap blocks per machine . = %d", ibmapBlocksPerMachine));
		System.out.println(String.format("Ibmaps total size MB ..... = %.3f", ibmapBlockSizeBytes*ibmapBlocksPerMachine/1024.0/1024.0));
		System.out.println();
		System.out.println(String.format("Inode block size MB ...... = %.3f", inodesBlockSizeBytes/1024.0/1024.0));
		System.out.println(String.format("Inode size bytes ......... = %d", inodeSizeBytes));
		System.out.println(String.format("Inodes per block ......... = %d", inodesPerBlock));
		System.out.println(String.format("Inode blocks per machine . = %d", inodeBlocksPerMachine));
		System.out.println(String.format("Inode blocks total size MB = %.3f", inodeBlocksPerMachine*inodesBlockSizeBytes/1024.0/1024.0));
	}
}
