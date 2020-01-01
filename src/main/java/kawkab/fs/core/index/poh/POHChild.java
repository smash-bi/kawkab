package kawkab.fs.core.index.poh;

import java.nio.ByteBuffer;

public class POHChild implements TimeRange {
	private int nodeNum;
	private long minTS = Long.MAX_VALUE;
	private long maxTS;

	POHChild() {
	}

	POHChild(final int nodeNumber, final long minTS, final long maxTS) {
		this.nodeNum = nodeNumber;
		this.minTS = minTS;
		this.maxTS = maxTS;
	}

	int nodeNumber() {
		return nodeNum;
	}

	static int sizeBytes() {
		return Integer.BYTES + Long.BYTES + Long.BYTES;
	}

	@Override
	public long minTS() {
		return minTS;
	}

	@Override
	public long maxTS() {
		return maxTS;
	}

	@Override
	public int compare(long ts) {
		if (minTS <= ts && ts <= maxTS)
			return 0;

		if (maxTS < ts)
			return -1;

		return 1;
	}

	int storeTo(ByteBuffer buffer){
		buffer.putInt(nodeNum);
		buffer.putLong(minTS);
		buffer.putLong(maxTS);

		return Integer.BYTES + Long.BYTES*2;
	}

	int loadFrom(ByteBuffer buffer) {
		nodeNum = buffer.getInt();
		minTS = buffer.getLong();
		maxTS = buffer.getLong();

		return Integer.BYTES + Long.BYTES*2;
	}
}
