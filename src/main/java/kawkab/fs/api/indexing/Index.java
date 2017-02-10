package kawkab.fs.api.indexing;

import kawkab.fs.api.Record;

public interface Index {
	public long get(Object data);
	public long get(Record record);
}
