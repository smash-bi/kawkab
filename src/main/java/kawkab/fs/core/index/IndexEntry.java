package kawkab.fs.core.index;

import java.nio.ByteBuffer;

public class IndexEntry {
	private long timestamp;
	private long byteOffsetInFile;
	
	public IndexEntry(long timestamp, long byteOffsetInFile) {
		this.timestamp = timestamp;
		this.byteOffsetInFile = byteOffsetInFile;
	}
	
	public void writeTo(ByteBuffer dstBuffer) {
		dstBuffer.putLong(timestamp);
		dstBuffer.putLong(byteOffsetInFile);
	}
	
	public void readFrom(ByteBuffer srcBuffer) {
		timestamp = srcBuffer.getLong();
		byteOffsetInFile = srcBuffer.getLong();
	}
	
	public long timestamp() {
		return timestamp;
	}
}
