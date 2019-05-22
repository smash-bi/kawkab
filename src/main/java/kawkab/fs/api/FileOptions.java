package kawkab.fs.api;

import java.io.File;

public final class FileOptions {
	final int recordSize;
	
	public enum FileType {
		BINARY,
		FIXED_LEN_RECORDS
		//public static final FileType values[] = values();
	}
	
	public FileOptions() { this(1); }
	
	public FileOptions(int recordSize){ this.recordSize = recordSize; }
	
	public static FileOptions defaults() { return new FileOptions(); }
	
	public int recordSize() { return recordSize; }
}
