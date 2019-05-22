package kawkab.fs.api;

import java.nio.ByteBuffer;

public interface Record {
	/**
	 * @return Returns the globally unique key of the record.
	 */
	long key();
	
	/**
	 * Returns a buffer to load data from the file.
	 *
	 * The function must set the position and limit values of the buffer before returning. The caller only writes
	 * size() bytes to the buffer. Therefore, the buffer must have at least size() bytes remaining to be written.
	 *
	 * The caller may or may not reset the position, mark, and limit values of the dstBuffer after reading from the buffer.
	 *
	 * @return Number of bytes read from the buffer
	 */
	ByteBuffer copyInDstBuffer();
	
	/**
	 * Returns a buffer to the Filesystem's writer, which is uses to copy the record to the virtual file.
	 *
	 * The function must set the position and limit values of the buffer before returning. The caller only copies
	 * size() bytes from the buffer. Therefore, the buffer must have at least size() bytes remaining.
	 *
	 * The caller may or may not reset the position, mark, and limit values of the dstBuffer after reading from the dstBuffer.
	 *
	 * @return the buffer that contains the record's data
	 */
	ByteBuffer copyOutSrcBuffer();
	
	/**
	 * The size of the record in bytes.
	 * @return
	 */
	int size();
}
