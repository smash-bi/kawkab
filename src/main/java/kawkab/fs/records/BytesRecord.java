package kawkab.fs.records;

import kawkab.fs.api.Record;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Random;

public class BytesRecord implements Record {
	private ByteBuffer buffer;
	private static final int TIMESTAMP;
	private final int sizeBytes;

	private final static int DEFAULT_SIZE = 100;

	static {
		TIMESTAMP 		= 0;
	}

	public BytesRecord() {
		this(System.currentTimeMillis(), DEFAULT_SIZE);
	}

	public BytesRecord(int recSize) {
		this(System.currentTimeMillis(), recSize);
	}

	public BytesRecord(long timestamp, int sizeBytes) {
		assert sizeBytes >= 16 : " Record size should be greater than 16, given "+sizeBytes;

		buffer = ByteBuffer.allocate(sizeBytes);
		timestamp(timestamp);
		this.sizeBytes = sizeBytes;

	}

	@Override
	public long timestamp() {
		return buffer.getLong(TIMESTAMP);
	}

	@Override
	public void timestamp(long newTimestamp) {
		buffer.putLong(TIMESTAMP, newTimestamp);
	}

	@Override
	public ByteBuffer copyInDstBuffer() {
		buffer.clear();
		return buffer;
	}

	@Override
	public ByteBuffer copyOutSrcBuffer() {
		buffer.clear();
		return buffer;
	}

	@Override
	public int size() {
		return sizeBytes;
	}

	@Override
	public Record newRecord() {
		return new BytesRecord(size());
	}

	public Record newRandomRecord(Random rand, long timestamp) {
		return new BytesRecord(timestamp, DEFAULT_SIZE);
	}

	public Record newRandomRecord(long timestamp, int sizeBytes) {
		return new BytesRecord(timestamp, sizeBytes);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		BytesRecord that = (BytesRecord) o;
		return Objects.equals(buffer, that.buffer);
	}

	@Override
	public int hashCode() {
		return Objects.hash(buffer);
	}
}
