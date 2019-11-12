package kawkab.fs.records;

import kawkab.fs.api.Record;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Random;

public class SixteenRecord implements Record {
	private ByteBuffer buffer; //The buffer that holds the record's fields in sequence

	//The following variables denote the byte start position of the corresponding field in the buffer
	private static final int TIMESTAMP;
	private static final int VALUE;

	//Size of this record in bytes
	private static final int SIZE;

	static {
		TIMESTAMP 		= 0;
		VALUE	 		= TIMESTAMP + Long.BYTES;

		SIZE = VALUE + Long.BYTES;
	}

	public SixteenRecord() {
		buffer = ByteBuffer.allocate(SIZE);
	}

	public SixteenRecord(final long timestamp, final long value) {
		this();

		buffer.putLong(timestamp);
		buffer.putLong(value);

		assert buffer.remaining() == 0;
		buffer.rewind();
	}

	@Override
	public long timestamp() {
		return buffer.getLong(TIMESTAMP);
	}

	@Override
	public void timestamp(long newTimestamp) {
		buffer.putLong(TIMESTAMP, newTimestamp);
	}

	public long value() {
		return buffer.getLong(VALUE);
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
		return SIZE;
	}

	@Override
	public Record newRecord() {
		return new SixteenRecord();
	}

	@Override
	public String toString() {
		return String.format("ts=%d,v=d",timestamp(), value());
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		SixteenRecord that = (SixteenRecord) o;
		return	this.size() 		== that.size() &&
				this.timestamp() 	== that.timestamp() &&
				this.value()	 	== that.value();
	}

	@Override
	public int hashCode() {
		return Objects.hash(buffer);
	}

	@Override
	public Record newRandomRecord(final Random rand, final long timestamp) {
		return new SixteenRecord(timestamp, rand.nextLong());
	}
}
