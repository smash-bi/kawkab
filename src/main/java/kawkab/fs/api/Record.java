package kawkab.fs.api;

import java.nio.ByteBuffer;

public interface Record {
	public long key();
	
	/**
	 * Reads data from the buffer and set values for this record
	 * @param buffer Input buffer: Data is read from this buffer
	 * @return Number of bytes read from the buffer
	 */
	public int read(ByteBuffer buffer);
	
	/**
	 * Writes data into the buffer
	 * @param buffer Output buffer: This record files are written into the buffer
	 * @return Number of bytes written into the buffer
	 */
	public int write(ByteBuffer buffer);
}
