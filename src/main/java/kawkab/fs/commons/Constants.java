package kawkab.fs.commons;

import kawkab.fs.core.IndexBlock;
import kawkab.fs.core.Inode;

public class Constants {
	//Default data block size in bytes
	public static final int dataBlockSizeBytes = 16*1024*1024;
	public static final int directBlocksPerInode = 16;
	public static final int numPointersInIndexBlock = dataBlockSizeBytes/IndexBlock.pointerSizeBytes;
	
	//Ibmap blocks range for this machine
	public static final int ibmapBlockSizeBytes = 16*1024; //FIXME: using a small number for testing
	public static final int ibmapBlocksPerMachine = 1; //FIXME: Calculate this based on the maximum number of files supported per machine
	public static int ibmapBlocksRangeStart = 0; //TODO: Get these numbers from a configuration file or ZooKeeper

	public static final int inodesBlockSizeBytes = dataBlockSizeBytes;
	public static final int inodeSizeBytes = Inode.inodesSize();
	public static final int inodesPerBlock = inodesBlockSizeBytes/inodeSizeBytes;
	public static final int inodeBlocksPerMachine = ibmapBlockSizeBytes*ibmapBlocksPerMachine*8/inodesPerBlock; //FIXME: Calculate this based on the maximum number of files supported by a machine
	public static int inodesBlocksRangeStart = 0; //TODO: Get these numbers from a configuration file or ZooKeeper
	
	public static int maxBlocksInCache = 1000;
	
	public static final String basePath = "/ssd1/sm3rizvi/kawkab";
	public static final String ibmapsPath = basePath+"/ibmaps";
	public static final String inodeBlocksPath = basePath+"/inodes";
	public static final String blocksPath = basePath+"/blocks";
	public static final String namespacePath = basePath+"/namespace";
	public static final int inodeBlocksPerDirectory = 1000;
	
	public static final long ibmapUuidHigh = 1; //High bits of uuid for ibmap. The low bits are the blockIndex
	public static final long inodesBlocksUuidHigh = 2;  //High bits of uuid for inodesBlock. The low bits are the blockIndex
	
	public static void printConfig(){
		System.out.println(String.format("Data block size MB ....... = %d",dataBlockSizeBytes/1024/1024));
		System.out.println(String.format("Direct blocks per inode .. = %d", directBlocksPerInode));
		System.out.println(String.format("Pointers per index block . = %d", numPointersInIndexBlock));
		System.out.println(String.format("Maximum file size MB ..... = %d", Inode.maxFileSize/1024/1024));
		System.out.println();
		System.out.println(String.format("Ibmap block size MB ...... = %d", ibmapBlockSizeBytes/1024/1024));
		System.out.println(String.format("Ibmap blocks per machine . = %d",ibmapBlocksPerMachine));
		System.out.println(String.format("Ibmaps total size MB ..... = %d", ibmapBlockSizeBytes*ibmapBlocksPerMachine/1024/1024));
		System.out.println();
		System.out.println(String.format("Inode block size MB ...... = %d", inodesBlockSizeBytes/1024/1024));
		System.out.println(String.format("Inode size bytes ......... = %d", inodeSizeBytes));
		System.out.println(String.format("Inodes per block ......... = %d", inodesPerBlock));
		System.out.println(String.format("Inode blocks per machine . = %d", inodeBlocksPerMachine));
		System.out.println(String.format("Inode blocks total size MB = %d", inodeBlocksPerMachine*inodesBlockSizeBytes/1024/1024));
	}
}
