package kawkab.fs.core;

import kawkab.fs.core.exceptions.KawkabException;
import kawkab.fs.core.index.FileIndex;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

public class IndexBlock implements FileIndex {
	@Override
	public long offsetInFile(long key) throws IOException, KawkabException {
		return 0;
	}
	
	@Override
	public void append(long key, long byteOffsetInFile) throws IOException, KawkabException {
	}
	
	public void openFile() {
	
	}
	
	public void closeFile() {
	
	}
	
	public void storeToFile(IndexSegmentID id, WritableByteChannel channel) {
	
	}
	
	public void loadFromFile(IndexSegmentID id, ReadableByteChannel channel) {
	
	}
	
}
