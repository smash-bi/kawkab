package kawkab.fs.commons;

/**
 * Utility functions for the files that have fixed length records
 */
public interface FixedLenRecordUtils {
	Configuration conf = Configuration.instance();
	int segmentSizeBytes = conf.segmentSizeBytes;
	
	/**
	 * Number of records of the given size that can fit in a segment
	 * @param recordSize
	 * @return
	 */
	static int recordsPerSegment(int recordSize) {
		return segmentSizeBytes / recordSize;
	}
	
	/**
	 * Is the given segment the last segment in the block
	 */
	static boolean isLastSegment(long segmentInFile, long fileSize, int recordSize) {
		return segmentInFile == segmentInFile(fileSize, recordSize);
	}
	
	/**
	 * Returns the segment number in the file that contains the given offset in the file
	 */
	static long segmentInFile(long offsetInFile, int recordSize) {
		// segmentInFile := recordInFile / recordsPerSegment
		return (offsetInFile/recordSize) / recordsPerSegment(recordSize);
	}
	
	/**
	 * Returns the block number in the file that contains the given segment number in the file
	 */
	static long blockInFile(long segmentInFile) {
		// blockInFile := segmentInFile x segmentSize / blockSize
		return segmentInFile * segmentSizeBytes / conf.dataBlockSizeBytes;
	}
	
	/**
	 * Returns the segment number in the block that corresponds to the given segment number in the file
	 */
	static int segmentInBlock(long segmentInFile) {
		// segmentInBlock := segmentInFile % segmentsPerBlock
		return (int)(segmentInFile % conf.segmentsPerBlock);
	}
	
	/**
	 * Returns the record number in its segment that contains the given offset in the file
	 */
	static int recordInSegment(long offsetInFile, int recordSize) {
		return (int)((offsetInFile/recordSize) % recordsPerSegment(recordSize));
	}
	
	/**
	 * Converts the offset in file to the offset in the segemnt
	 */
	static int offsetInSegment(long offsetInFile, int recordSize) {
		// offsetInSegment = recordInSegment + offsetInRecord
		return (int)(recordInSegment(offsetInFile, recordSize) + (offsetInFile % recordSize));
	}
	
	/**
	 * Converts the offset in the file to the offset in the block
	 */
	static int offsetInBlock(long offsetInFile, int recordSize) {
		// offsetInBlock = segmentInBlock + recordInSegment + offsetInRecord
		return (int)((segmentInBlock(segmentInFile(offsetInFile, recordSize)) + recordInSegment(offsetInFile, recordSize) + (offsetInFile % recordSize)));
	}
}
