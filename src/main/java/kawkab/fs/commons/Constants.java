package kawkab.fs.commons;

import kawkab.fs.core.IndexBlock;

public class Constants {
	//Default data block size in bytes
	public static final int dataBlockSizeBytes = 4 * 1024 * 1024; //4MB
	public static final int maxDirectBlocks = 16;
	public static final int numPointersInIndexBlock = dataBlockSizeBytes/IndexBlock.pointerSizeBytes;
	public static final long fileSizeLimit = Long.MAX_VALUE; //The maximum file size depends on numPointersInIndexBlock and maxIndexLevels.
	
	public static final int inodesBlockSizeBytes = 1024; //4 * 1024 * 1024;
	public static final int inodeSizeBytes = maxDirectBlocks*IndexBlock.pointerSizeBytes + 3*IndexBlock.pointerSizeBytes + 32; //pointers + variables + reserved
	public static final int inodesPerBlock = inodesBlockSizeBytes / inodeSizeBytes;
	public static int inodesBlocksRangeStart = 0;
	public static int inodesBlocksRangeEnd = 0; //10;
	
	//Ibmap blocks range for this machine
	public static int ibmapBlocksRangeStart = 0; //TODO: Get these numbers from a configuration file or ZooKeeper
	public static int ibmapBlocksRangeEnd = 0; //10;
	public static final int ibmapBlockSizeBytes = 1*1024*1024; //4 * 1024 * 1024;
	public static final int ibmapBlocksPerMachine = 20; //10; //FIXME: Calculate this based on the maximum 
	                                                            //number of files supported by a machine
	public static final String fsBasePath = "/tmp/kawkab";
	public static final String ibmapsPath = "ibmaps";
	public static final String blocksPath = "blocks";
}
