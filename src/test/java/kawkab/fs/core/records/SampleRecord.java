package kawkab.fs.core.records;

import kawkab.fs.api.Record;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Random;

public class SampleRecord implements Record {
	private ByteBuffer buffer; //The buffer that holds the record's fields in sequence
	
	//The following variables denote the byte start position of the corresponding field in the buffer
	private static final int TIMESTAMP;
	private static final int BID_PRICE;
	private static final int BID_QUANTITY;
	private static final int BID_EXECUTABLE;
	private static final int ASK_PRICE;
	private static final int ASK_QUANTITY;
	private static final int ASK_EXECUTABLE;
	
	private static byte TRUE  = 1;
	private static byte FALSE = 0;
	
	//Size of this record in bytes
	private static final int SIZE;
	
	static {
		TIMESTAMP 		= 0;
		BID_PRICE 		= TIMESTAMP + Long.BYTES;
		BID_QUANTITY 	= BID_PRICE + Double.BYTES;
		BID_EXECUTABLE	= BID_QUANTITY + Double.BYTES;
		ASK_PRICE		= BID_EXECUTABLE + Byte.BYTES;
		ASK_QUANTITY	= ASK_PRICE + Double.BYTES;
		ASK_EXECUTABLE	= ASK_QUANTITY + Double.BYTES;
		
		SIZE = ASK_EXECUTABLE + Byte.BYTES;
	}
	
	public SampleRecord () {
		buffer = ByteBuffer.allocate(SIZE);
	}

	public SampleRecord(long timestamp, double bidPrice, double bidQuantity, boolean bidExecutable, double askPrice, double askQuantity, boolean askExecutable) {
		this();
		
		buffer.putLong(timestamp);
		buffer.putDouble(bidPrice);
		buffer.putDouble(bidQuantity);
		buffer.put(bidExecutable?TRUE:FALSE);
		buffer.putDouble(askPrice);
		buffer.putDouble(askQuantity);
		buffer.put(askExecutable?TRUE:FALSE);
		
		assert buffer.remaining() == 0;
		buffer.rewind();
	}
	
	@Override
	public long timestamp() {
		return buffer.getLong(TIMESTAMP);
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

	public static int length() {
		return SIZE;
	}
	
	public double bidPrice() {
		return buffer.getDouble(BID_PRICE);
	}
	
	public double bidQuantity() {
		return buffer.getDouble(BID_QUANTITY);
	}
	
	public boolean bidExecutable() {
		return buffer.get(BID_EXECUTABLE) == TRUE;
	}
	
	public double askPrice() {
		return buffer.getDouble(ASK_PRICE);
	}
	
	public double askQuantity() {
		return buffer.getDouble(ASK_QUANTITY);
	}
	
	public boolean askExecutable() {
		return buffer.get(ASK_EXECUTABLE) == TRUE;
	}
	
	@Override
	public String toString() {
		return String.format("ts=%d,bp=%.2f,bq=%.2f,be=%s,ap=%.2f,aq=%.2f,ae=%s",
				timestamp(), bidPrice(), bidQuantity(), bidExecutable(), askPrice(), askQuantity(), askExecutable());
	}
	
	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		SampleRecord that = (SampleRecord) o;
		return	this.size() 		== that.size() &&
				this.timestamp() 	== that.timestamp() &&
				this.bidPrice() 	== that.bidPrice() &&
				this.bidQuantity() 	== that.bidQuantity() &&
				this.bidExecutable() == that.bidExecutable() &&
				this.askPrice() 	== that.askPrice() &&
				this.askQuantity() 	== that.askQuantity() &&
				this.askExecutable() == that.askExecutable();
	}
	
	@Override
	public int hashCode() {
		return Objects.hash(buffer);
	}

	@Override
	public Record newRecord() {
		return new SampleRecord();
	}

	@Override
	public Record newRandomRecord(final Random rand, final long timestamp) {
		assert rand != null;
		assert timestamp >= 0;

		return new SampleRecord(timestamp, rand.nextDouble(), rand.nextDouble(), rand.nextBoolean(),
				rand.nextDouble(), rand.nextDouble(), rand.nextBoolean());
	}
}
