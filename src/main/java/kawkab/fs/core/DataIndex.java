package kawkab.fs.core;

public class DataIndex {
	private final long offsetInFile; //Offset in the file
	private final int length; //Length of data
	
	//Time when the data was appended; used for indexing within the filesystem
	//Clients can use this time to access the data
	private final long timestamp;
	
	private DataIndex(long byteOffset, long timestamp, int length){
		this.offsetInFile = byteOffset;
		this.timestamp = timestamp;
		this.length = length;
	}
	
	public long offsetInFile(){
		return offsetInFile;
	}
	
	public long timestamp(){
		return timestamp;
	}
	
	public int length(){
		return length;
	}
	
	public boolean hasByte(long fileOffset){
		if (fileOffset >= offsetInFile && fileOffset <= offsetInFile+length)
			return true;
		return false;
	}
}
