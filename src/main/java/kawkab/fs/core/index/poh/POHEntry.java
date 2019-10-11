package kawkab.fs.core.index.poh;

/**
 * This class is not thread-safe. There can be a race while reading and writing maxTS and isMaxSet.
 */
class POHEntry implements TimeRange{
	private final long segmentInFile;
	private final long minTS;
	private long maxTS;
	private boolean isMaxSet;

	POHEntry(final long minTS, final long segmentInFile) {
		this(minTS, minTS, segmentInFile);
		isMaxSet = false;
	}

	POHEntry(final long minTS, final long maxTS, final long segmentInFile) {
		assert minTS <= maxTS;

		this.segmentInFile = segmentInFile;
		this.minTS = minTS;
		this.maxTS = maxTS;
		isMaxSet = true;
	}

	void setMaxTS(final long maxTS) {
		assert !isMaxSet;
		assert minTS <= maxTS;

		this.maxTS = maxTS;
		isMaxSet = true;
	}

	long segmentInFile() {
		return segmentInFile;
	}

	static int sizeBytes() {
		return Long.BYTES * 3; // Two timestamps and file offset
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

	@Override
	public long minTS() {
		return minTS;
	}

	@Override
	public long maxTS() {
		return maxTS;
	}
}
