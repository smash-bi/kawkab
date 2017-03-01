package kawkab.fs.core;

public class FileOffset {
	private long timestamp;
	private long fileOffset;
	
	public FileOffset(long fileOffset, long timestamp){
		this.fileOffset = fileOffset;
		this.timestamp = timestamp;
	}
	
	/**
	 * @return The timestamp associated with the offsetInFile() byte of the file.
	 */
	public long timestamp(){
		return timestamp;
	}
	
	/**
	 * @return Offset relative to the start of the file, i.e., absolute byte number in a file.
	 */
	public long offsetInFile(){
		return fileOffset;
	}
}
