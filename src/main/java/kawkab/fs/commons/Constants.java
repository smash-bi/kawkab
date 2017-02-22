package kawkab.fs.commons;

public class Constants {
	//Default data block size in bytes
	public static final int defaultBlockSize = 256 * 1024 * 1024; //256MB
	
	/*
	 * The maximum file size supported is 2^63-1. If a data block is 256MB, the file can have at most
	 * 34359738367 blocks. A k-ary tree of height h has h^k leaves. We want to fit the index in
	 * at most two levels. Therefore, we can fit all the blocks if the number of pointers N in an
	 * index block is:
	 *    N = log_h(34359738367) 
	 *      = 35, where h=2.
	 * Change this value according to the data block size and the maximum number of levels in the index blocks.
	 */
	//public static final int maxIndexLevels = 2;
	//public static final int numPointersInIndexBlock = 35;
	
	/*
	 * Instead of providing all blocks through two level index, we can support a 256GB file
	 * using 1024 direct data pointers to 256MB blocks. If the file
	 * exceeds this size, we can create indirect blocks.
	 */
	public static final int maxIndexLevels = 2;
	public static final int maxDirectBlocks = 32;
	public static final int numPointersInIndexBlock = 512;
	public static final long fileSizeLimit = Long.MAX_VALUE;
}
