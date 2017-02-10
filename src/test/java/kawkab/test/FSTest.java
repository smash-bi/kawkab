package kawkab.test;

import java.nio.ByteBuffer;

import kawkab.fs.api.FileOptions;
import kawkab.fs.api.FilePath;
import kawkab.fs.api.Filesystem;
import kawkab.fs.api.Record;
import kawkab.fs.client.FileHandle;

public class FSTest {
	public static void main(String args[]) {
		Filesystem fs = Filesystem.instance();
		
		FilePath path = new FilePath("/home/smash/test");
		FileOptions opts = new FileOptions();
		
		FileHandle file = fs.create(path, opts);
		
		
		String data = "sample data";
		ByteBuffer buf = ByteBuffer.wrap(data.getBytes());
		
		file.append(buf, 0, buf.capacity());
		ByteBuffer rdata = file.read(1);
		
		//////////////////////////////////////////////
		// Another way to read/write data
		
		Record record = new Record(){
			long timestamp;
			int bid;
			int ask;
			int commodityID;
			
			@Override
			public long key() { return timestamp; }

			@Override
			public int read(ByteBuffer buffer) {
				int len = 0;
				timestamp = buffer.getLong();
				bid = buffer.getInt();
				ask = buffer.getInt();
				commodityID = buffer.getInt();
				return 20;
			}

			@Override
			public int write(ByteBuffer buffer) {
				buffer.putLong(timestamp);
				buffer.putInt(bid);
				buffer.putInt(ask);
				buffer.putInt(commodityID);
				return 0;
			}
		};
		
		file.appendRecord(record);
		
		Record readRecord = file.readRecord(record.key());
	}
}
