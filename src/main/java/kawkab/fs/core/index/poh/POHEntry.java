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
	private boolean isMinStored;
	private boolean isDirty;

	POHEntry() {
		segmentInFile = -1;
		minTS = -1;
		maxTS = -1;
		isMaxSet = false;
		isMinStored = false;
		isDirty = false;
	}

	POHEntry(final long minTS, final long segmentInFile) {
		this(minTS, minTS, segmentInFile);
		isMaxSet = false;
		isDirty = true;
	}

	POHEntry(final long minTS, final long maxTS, final long segmentInFile) {
		assert minTS <= maxTS;

		this.segmentInFile = segmentInFile;
		this.minTS = minTS;
		this.maxTS = maxTS;
		isMaxSet = true;
		isDirty = true;
	}

	void setMaxTS(final long maxTS) {
		assert !isMaxSet;
		assert minTS <= maxTS : String.format("minsTS should less than or equal to the given maxTS, minTS=%d, maxTS=%d", minTS, maxTS);

		this.maxTS = maxTS;
		isMaxSet = true;
		isDirty = true;
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

	boolean storeTo(ByteBuffer dstBuf) {
		assert isDirty;

		if (!isMinStored) {
			dstBuf.putLong(segmentInFile);
			dstBuf.putLong(minTS);
			isMinStored = true;
		}

		if (isMaxSet) {
			dstBuf.putLong(maxTS);
		}

		isDirty = false;
		return isMaxSet;
	}

	boolean loadFrom(ByteBuffer srcBuf) {
		segmentInFile = srcBuf.getLong();
		minTS = srcBuf.getLong();
		isMinStored = true;
		isDirty = false;
		isMaxSet = false;

		if (srcBuf.remaining() >= Long.BYTES) {
			maxTS = srcBuf.getLong();

			if (maxTS > 0) { //If maxTS is zero, this indicates that the this entry had no maxTS when the node was persisted.
				isMaxSet = true;
			}
		}

		if (!isMaxSet) {
			maxTS = minTS;
		}

		return isMaxSet;
	}

	boolean isMaxSet() {
		return isMaxSet;
	}
}
