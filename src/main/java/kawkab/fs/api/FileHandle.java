package kawkab.fs.api;

public final class FileHandle {
	private String filename;
	private FileOptions options;
	
	public FileHandle(String filename, FileOptions options){
		this.filename = filename;
		this.options = options;
	}
	
	/**
	 * Reads data into the buffer
	 * @param buffer
	 * @param length Number of bytes to read from the file
	 * @return Number of bytes read from the file
	 */
	public int read(byte[] buffer, int length){
		return 0;
	}
	
	/**
	 * Seek the read pointer to the absolute bytes byteOffset in the file
	 * @param byteOffset
	 */
	public void seekBytes(long byteOffset){
	}
	
	/**
	 * Seek the read pointer to the first byte that was appended at or after time timestamp
	 * @param tiemstamp
	 */
	public void seekTime(long tiemstamp){
	}
	
	/**
	 * Move the read pointer to numBytes relative to its current position
	 * @param bytes number of bytes to move the read pointer
	 */
	public void relativeSeek(int numBytes){
		
	}
	
	/**
	 * Append data at the end of the file
	 * @param data Data to append
	 * @param offset Offset in the data array
	 * @param length Number of bytes to write from the data array
	 * @return Index of the data appended to the file. The caller can use 
	 * dataIndex.timestamp() to refer to the data just written. 
	 */
	public DataIndex append(byte[] data, int offset, int length){
		return new DataIndex(0, 0);
	}
}
