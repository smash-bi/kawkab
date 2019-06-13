package kawkab.fs.core.index;

import kawkab.fs.commons.Configuration;
import kawkab.fs.core.Cache;
import kawkab.fs.core.IndexSegment;
import kawkab.fs.core.IndexSegmentID;
import kawkab.fs.core.exceptions.KawkabException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

public class TimeSeriesIndex implements FileIndex {
	// Persistent fields
	private long lastTimestamp;
	private long lastTSByteOffset;
	private long indexLength; //Number of entries in the index
	private long rootBlock;
	
	// Other fields
	private final long inumber;
	private final int recordSize;
	
	// Static fields
	private static final int SIZE = 20;
	private static final Cache cache = Cache.instance();
	private static final Configuration conf = Configuration.instance();
	
	public TimeSeriesIndex(long inumber, int recordSize) {
		this.inumber = inumber;
		this.recordSize = recordSize;
	}
	
	@Override
	public void append(long key, long byteOffsetInFile) throws IOException, KawkabException {
		int blockIndex = blockIndexFromOffset(byteOffsetInFile, recordSize);
		IndexSegmentID indexSegmentID = getID(inumber, blockIndex);
		
		IndexSegment block = null;
		try {
			block = (IndexSegment) cache.acquireBlock(indexSegmentID);
			block.append(key, byteOffsetInFile);
			lastTimestamp = key;
			lastTSByteOffset = byteOffsetInFile;
		} finally {
			if (block != null) {
				cache.releaseBlock(indexSegmentID);
			}
		}
	}
	
	/**
	 *
	 * @param key Timestamp of the record
	 * @return the byte offset in the file if the key is found, otherwise returns a negative value
	 * @throws IOException
	 * @throws KawkabException
	 */
	@Override
	public long offsetInFile(long key) throws IOException, KawkabException {
		int blockIndex = blockIndexFromKey(key);
		IndexSegmentID indexSegmentID = getID(inumber, blockIndex);
		IndexSegment block = null;
		
		try {
			block = (IndexSegment) cache.acquireBlock(indexSegmentID);
			return block.offsetInFile(key);
		} finally {
			if (block != null) {
				cache.releaseBlock(indexSegmentID);
			}
		}
	}
	
	private int blockIndexFromKey(long key) {
		// FIXME: TBD
		return 0;
	}
	
	private int blockIndexFromOffset(long byteOffsetInFile, int recordSize) {
		int entriesPerBlock = conf.indexBlockSizeBytes / SIZE;
		int recordInFile = (int) (byteOffsetInFile / recordSize);
		return recordInFile / entriesPerBlock;
	}
	
	private IndexSegmentID getID(long inumber, int blockIndex) {
		return new IndexSegmentID(inumber, blockIndex);
	}
	
	/**
	 * @param srcBuffer
	 * @return Number of bytes read from the buffer
	 * @throws IOException
	 */
	int loadFrom(final ByteBuffer srcBuffer) throws IOException {
		assert srcBuffer.remaining() >= SIZE;
		
		return 0;
	}
	
	/**
	 * Loads inode variables from the channel
	 * @param srcChannel
	 * @return Number of bytes read from the channel
	 * @throws IOException
	 */
	int loadFrom(final ReadableByteChannel srcChannel) throws IOException {
		return 0;
	}
	
	/*
	 * Serializes the inode in the channel
	 * @return Number of bytes written in the channel
	 */
	int storeTo(final WritableByteChannel dstChannel) throws IOException {
		return 0;
	}
	
	int storeTo(ByteBuffer dstBuffer) {
		return 0;
	}
}
