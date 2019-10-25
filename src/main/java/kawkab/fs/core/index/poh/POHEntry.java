package kawkab.fs.core.index.poh;

import java.nio.ByteBuffer;

/**
 * This class is not thread-safe. There can be a race while reading and writing maxTS and isMaxSet.
 */
class POHEntry implements TimeRange{
	private long segmentInFile;
	private long minTS;
	private long maxTS;
	private boolean isMaxSet;

	POHEntry() {
		segmentInFile = -1;
		minTS = -1;
		maxTS = -1;
		isMaxSet = false;
	}

	POHEntry(final long minTS, final long segmentInFile) {
		this(minTS, minTS, segmentInFile);
		isMaxSet = false;
	}

	POHEntry(final long minTS, final long maxTS, final long segmentInFile) {
		assert minTS <= maxTS;

		System.out.println("\t\t\tEntry: "  + segmentInFile + " - " + minTS);

		this.segmentInFile = segmentInFile;
		this.minTS = minTS;
		this.maxTS = maxTS;
		isMaxSet = true;
	}

	void setMaxTS(final long maxTS) {
		assert !isMaxSet;
		assert minTS <= maxTS : String.format("minTS should less than or equal to the given maxTS, minTS=%d, maxTS=%d", minTS, maxTS);

		this.maxTS = maxTS;
		isMaxSet = true;

		System.out.println("\t\t\tEntry with max: " + segmentInFile + " - " + minTS + " - " + maxTS);
	}

	static int sizeBytes() {
		return Long.BYTES * 3; // Two timestamps and a file offset
	}

	@Override
	public String toString() {
		return String.format("ts=%d, segmentInFile=%d", minTS, segmentInFile);
	}

	@Override
	public int compare(final long timestamp) {
		//return minTS == timestamp ? 0 : (minTS < timestamp ? -1 : 1);
		if (minTS <= timestamp && timestamp <= maxTS)
			return 0;

		if (timestamp < minTS)
			return 1;

		return -1;
	}

	long segmentInFile() {
		return segmentInFile;
	}

	@Override
	public long minTS() {
		return minTS;
	}

	@Override
	public long maxTS() {
		return maxTS;
	}

	/**
	 * This function changes the internal state. Therefore, it is only called when storing data to file for persistence.
	 *
	 * @param dstBuf
	 * @return whehter the max value is stored in the buffer or not
	 */
	boolean storeTo(ByteBuffer dstBuf, boolean withMin) {
		if (withMin) { //If the offset is an even number, the buffer starts with the minTS and segInFile
			dstBuf.putLong(segmentInFile);
			dstBuf.putLong(minTS);
			System.out.printf("\t\t\t\t\tStoring: sif=%d, min=%d", segmentInFile, minTS);
		}

		if (isMaxSet) {
			dstBuf.putLong(maxTS);

			System.out.printf("\t >, max=%d\n", maxTS);

			return true;
		}

		 System.out.println();

		return false;
	}

	/**
	 * This function changes the internal state. Therefore, it must be only called when loading data from a persistent file or block.
	 * @param srcBuf
	 * @return
	 */
	boolean loadFrom(ByteBuffer srcBuf, boolean hasMin) {
		if (hasMin) {
			assert !isMaxSet;

			segmentInFile = srcBuf.getLong();
			minTS = srcBuf.getLong();
			maxTS = minTS;
		}

		if (srcBuf.remaining() >= Long.BYTES) {
			long max = srcBuf.getLong();

			if (max > 0) { //If maxTS is zero, this indicates that the this entry had no maxTS when the node was persisted.
				maxTS = max;
				isMaxSet = true;

				  System.out.printf("\t\t\t\t\tLoading with max: sif=%d, min=%d, max=%d\n", segmentInFile, minTS, maxTS);

				return true;
			}

			srcBuf.position(srcBuf.position() - Long.BYTES);
		}

		  System.out.printf("\t\t\t\t\tLoading: sif=%d, min=%d, max=%d\n", segmentInFile, minTS, maxTS);

		return false;
	}

	boolean isMaxSet() {
		return isMaxSet;
	}
}
