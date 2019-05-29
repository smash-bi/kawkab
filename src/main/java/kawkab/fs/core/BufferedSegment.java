package kawkab.fs.core;

import kawkab.fs.core.exceptions.KawkabException;
import kawkab.fs.core.timerqueue.TimerQueueItem;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Call append() only after calling freeze()
 *
 * Do not call deferredWork() directly.
 */
public class BufferedSegment extends TimerQueueItem {
	private DataSegment ds;
	private static final Cache cache = Cache.instance();
	
	public BufferedSegment(DataSegmentID dsid, long fileSize, int recordSize) throws IOException, KawkabException {
		ds = (DataSegment) cache.acquireBlock(dsid);
		ds.initForAppend(fileSize, recordSize);
	}
	
	/**
	 * @param data Data to be appended
	 * @param offset  Offset in data
	 * @param length  Number of bytes to append: data[offset] to data[offset+length-1] inclusive.
	 * @param offsetInFile
	 * @return number of bytes appended starting from the offset
	 * @throws IOException
	 */
	public int append(byte[] data, int offset, int length, long offsetInFile) throws IOException {
		verifyDisabled();
		return ds.append(data, offset, length, offsetInFile);
	}
	
	public int append(final ByteBuffer srcBuffer, long offsetInFile, int recordSize) throws IOException {
		verifyDisabled();
		return ds.append(srcBuffer, offsetInFile, recordSize);
	}
	
	
	public boolean isFull() {
		return ds.isFull();
	}
	
	@Override
	protected void deferredWork() throws KawkabException {
		DataSegmentID dsid = (DataSegmentID) ds.id();
		ds = null;
		cache.releaseBlock(dsid);
	}
	
	@Override
	public String toString() {
		return ds.id().toString();
	}
}
