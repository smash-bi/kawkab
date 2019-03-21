package kawkab.fs.core;

public class DataBuffer {
	private long fileOffset;
	private long size;
	private byte[] data;
	private int pos;
	private final int startPos;
	
	public DataBuffer (long fileOffset, int startPos) {
		this.fileOffset = fileOffset;
		this.startPos = startPos;
	}
	
	public void append(byte[] toCopy, int offset, int length) {
		if (offset + length > toCopy.length) {
			
		}
	}
}
