package kawkab.fs.core;

import kawkab.fs.api.Record;

import java.nio.ByteBuffer;
import java.util.Objects;

public class SampleRecord implements Record {
	private final ByteBuffer buffer; //The buffer that holds the record's fields in sequence
	
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
		BID_QUANTITY 	= BID_PRICE + Float.BYTES;
		BID_EXECUTABLE	= BID_QUANTITY + Float.BYTES;
		ASK_PRICE		= BID_EXECUTABLE + Byte.BYTES;
		ASK_QUANTITY	= ASK_PRICE + Float.BYTES;
		ASK_EXECUTABLE	= ASK_QUANTITY + Float.BYTES;
		
		SIZE = ASK_EXECUTABLE + Byte.BYTES;
	}
	
	public SampleRecord () {
		buffer = ByteBuffer.allocate(SIZE);
	}
	
	public SampleRecord(long timestamp, float bidPrice, float bidQuantity, boolean bidExecutable, float askPrice, float askQuantity, boolean askExecutable) {
		buffer = ByteBuffer.allocate(SIZE);
		
		buffer.putLong(timestamp);
		buffer.putFloat(bidPrice);
		buffer.putFloat(bidQuantity);
		buffer.put(bidExecutable?TRUE:FALSE);
		buffer.putFloat(askPrice);
		buffer.putFloat(askQuantity);
		buffer.put(askExecutable?TRUE:FALSE);
		
		assert buffer.remaining() == 0;
		buffer.rewind();
	}
	
	@Override
	public long key() {
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
	
	public long timestamp() {
		return buffer.getLong(TIMESTAMP);
	}
	
	public float bidPrice() {
		return buffer.getFloat(BID_PRICE);
	}
	
	public float bidQuantity() {
		return buffer.getFloat(BID_QUANTITY);
	}
	
	public boolean bidExecutable() {
		return buffer.get(BID_EXECUTABLE) == TRUE;
	}
	
	public float askPrice() {
		return buffer.getFloat(ASK_PRICE);
	}
	
	public float askQuantity() {
		return buffer.getFloat(ASK_QUANTITY);
	}
	
	public boolean askExecutable() {
		return buffer.get(ASK_EXECUTABLE) == TRUE;
	}
	
	@Override
	public String toString() {
		return String.format("ts=%d,bp=%.2f,bq=%.2f,be=%s,ap=%.2f,aq=%.2f,ae=%s\n",
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
}
