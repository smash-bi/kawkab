package kawkab.fs.core.index.poh;

class POHEntry implements TimeRange{
	private final long offsetInFile;
	private final long ts;

	POHEntry(final long timestamp, final long offsetInFile) {
		this.offsetInFile = offsetInFile;
		this.ts = timestamp;
	}

	long segmentInFile() {
		return offsetInFile;
	}

	long timestamp() {
		return ts;
	}

	static int sizeBytes() {
		return Long.BYTES * 3; // Two timestamps and file offset
	}

	@Override
	public String toString() {
		return String.format("ts=%d, offsetInFile=%d", ts, offsetInFile);
	}

	@Override
	public int compare(long timestamp) {
		return ts == timestamp ? 0 : (ts < timestamp ? -1 : 1);
	}

	@Override
	public long minTS() {
		return ts;
	}

	@Override
	public long maxTS() {
		return ts;
	}
}
