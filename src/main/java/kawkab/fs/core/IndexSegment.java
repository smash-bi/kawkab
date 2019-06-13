package kawkab.fs.core;

import com.google.protobuf.ByteString;
import kawkab.fs.commons.Configuration;
import kawkab.fs.core.exceptions.FileNotExistException;
import kawkab.fs.core.exceptions.KawkabException;
import kawkab.fs.core.index.FileIndex;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;

public class IndexSegment extends Block implements FileIndex {
	private class Entry {
		long key; long value;
		Entry(long key, long value){this.key = key; this.value = value;};
	}
	
	private Entry[] indices;
	private int indexPos = 0;
	
	private static Configuration conf = Configuration.instance();
	
	IndexSegment(BlockID id){
		super(id);
		indices = new Entry[conf.indexBlockSizeBytes/Long.BYTES];
	}
	
	/**
	 * @param key
	 * @return the byte offset in the file if the key is found, otherwise returns a negative value
	 */
	@Override
	public long offsetInFile(long key) {
		// FIXME: Perform binary search
		
		int maxPos = indexPos;
		
		for (int i=0; i<maxPos; i+=2) {
			if (indices[i].key == key)
				return indices[i].value;
		}
		
		return -1;
	}
	
	@Override
	public void append(long key, long byteOffsetInFile) {
		assert indices.length > indexPos +1;
		
		indices[indexPos] = new Entry(key,byteOffsetInFile);
		indexPos++;
		markLocalDirty();
	}
	
	@Override
	protected boolean shouldStoreGlobally() {
		return indexPos == conf.indexBlockSizeBytes;
	}
	
	@Override
	public int loadFromFile() throws IOException {
		return 0;
	}
	
	@Override
	public int loadFrom(ByteBuffer buffer) throws IOException {
		return 0;
	}
	
	@Override
	public int loadFrom(ReadableByteChannel channel) throws IOException {
		return 0;
	}
	
	@Override
	int storeTo(FileChannel channel) throws IOException {
		return 0;
	}
	
	@Override
	int storeToFile() throws IOException {
		return 0;
	}
	
	@Override
	public ByteString byteString() {
		return null;
	}
	
	@Override
	public int sizeWhenSerialized() {
		return 0;
	}
	
	@Override
	public boolean evictLocallyOnMemoryEviction() {
		return false;
	}
	
	@Override
	protected void loadBlockOnNonPrimary() throws FileNotExistException, KawkabException, IOException {
	
	}
	
	@Override
	protected void loadBlockFromPrimary() throws FileNotExistException, KawkabException, IOException {
	}
	
	@Override
	void onMemoryEviction() {
	}
}
