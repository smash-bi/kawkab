package kawkab.fs.api;

public class DataOffset {
	private final long fileOffset;
	private final long dataSize;
	
	/**
	 * Offset and length of data
	 * @param fileOffset Offset relative to start of the file
	 * @param size Data size in bytes
	 */
	public DataOffset(long fileOffset, long size){
		this.fileOffset = fileOffset;
		this.dataSize = size;
	}
	
	public long offset(){
		return fileOffset;
	}
	
	public long size(){
		return dataSize;
	}
}
