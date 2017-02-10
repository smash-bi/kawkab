package kawkab.test;

import kawkab.fs.api.FileHandle;
import kawkab.fs.api.DataIndex;
import kawkab.fs.api.FileOptions;
import kawkab.fs.api.Filesystem;
import kawkab.fs.api.Filesystem.FileMode;

public class FSTest {
	public static void main(String args[]) {
		Filesystem fs = Filesystem.instance();
		
		String filename = new String("/home/smash/test");
		FileOptions opts = new FileOptions();
		
		FileHandle file = fs.open(filename, FileMode.APPEND, opts);
		
		String data = "sample data";
		byte[] dataBuffer = data.getBytes();
		
		DataIndex index = file.append(dataBuffer, 0, dataBuffer.length);
		
		file.seekBytes(10);
		file.read(dataBuffer, 100);
		
		file.seekTime(index.timestamp());
		file.read(dataBuffer, 100);
	}
}
