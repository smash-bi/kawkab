package kawkab.fs.api;

public class DataIndex {
	//Offset in the file
	private final long byteOffset;
	
	//Time when the data was appended; used for indexing within the filesystem
	//Clients can use this time to access the data
	private final long timestamp;
	
	public DataIndex(long byteOffset, long timestamp){
		this.byteOffset = byteOffset;
		this.timestamp = timestamp;
	}
	
	public long byteOffset(){
		return byteOffset;
	}
	
	public long timestamp(){
		return timestamp;
	}
	
}
