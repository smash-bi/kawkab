package kawkab.fs.client;

import java.nio.ByteBuffer;
import java.util.List;

import kawkab.fs.api.FileOptions;
import kawkab.fs.api.FilePath;
import kawkab.fs.api.Record;
import kawkab.fs.api.indexing.Index;

public final class FileHandle {
	private FilePath path;
	private FileOptions options;
	private Index index;
	//private FileMetadata metadata;
	//private FileSystemContext fsContext;
	
	///////////////////////////////////////////////////////
	
	public FileHandle(FilePath path, FileOptions options){
		this.path = path;
		this.options = options;
		this.index = options.index();
	}
	
	public ByteBuffer read(long timestamp){
		return null;
	}
	
	public int append(ByteBuffer data, int offset, int length){
		long appendAt = index.get(data);
		
		return 0;
	}
	
	//////////////////////////////////////////////////////////////
	
	/**
	 * Reads a record from the file at location index.
	 * @param index Position of the record in the file
	 * @return Record retrieved from the file
	 */
	public Record readRecord(long index){
		return null;
	}
	
	/**
	 * Appends the record in this file.
	 * @param record The record to append in the file.
	 * @return Whether the operation was successful or not
	 */
	public boolean appendRecord(Record record){
		long appendAt = index.get(record);
		return false;
	}
	
	/**
	 * Appends a sequence of records in this file.
	 * @param record The ordered list of records to append in the file.
	 * @return Number of records that were successfully appended in the file.
	 */
	public int appendRecord(List<Record> record){
		return 0;
	}
	
	public void close(){}
	
	public Record readNext(long time){
		return null;
	}
	
	public Record readBefore(long time){
		return null;
	}
	
	public List<Record> readBetween(long time1, long time2){
		return null;
	}
}
