package kawkab.fs.api.indexing;

import kawkab.fs.api.Record;

public class TimeIndex implements Index {
	@Override
	public long get(Object data) {
		return 0;
	}

	@Override
	public long get(Record record) {
		long key = record.key();
		
		//Process key
		long fsIndex = key;
		
		return fsIndex;
	}
}
