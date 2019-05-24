package kawkab.fs.core;

import com.google.protobuf.ByteString;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

public interface Externable {
	/**
	 * Indicates if the block is allowed be stored in the global store. Dirty bits are tested separately.
	 * @return
	 */
	boolean shouldStoreGlobally();
	
	/**
	 * Load the content of the block from the local file
	 *
	 * @return Number of bytes loaded from the file
	 * @throws IOException
	 */
	int loadFromFile() throws IOException;
	
	/**
	 * Load the content of the block from the given ByteBuffer
	 *
	 * @param buffer
	 *
	 * @return Number of bytes read from the buffer
	 * @throws IOException
	 */
	int loadFrom(ByteBuffer buffer) throws IOException;
	
	/**
	 * Load the contents of the block from channel.
	 *
	 * @param channel
	 *
	 * @return Number of bytes read from the channel
	 * @throws IOException
	 */
	int loadFrom(ReadableByteChannel channel)  throws IOException;
	
	/**
	 * Store contents of the block to channel. This function stores only the
	 * updated bytes in the channel.
	 *
	 * @param channel
	 * @return Number of bytes written to the channel.
	 * @throws IOException
	 */
	int storeTo(WritableByteChannel channel)  throws IOException;
	
	/**
	 * Stores the block in a file.
	 * @return Number of bytes written in the file.
	 */
	int storeToFile() throws IOException;
	
	/**
	 * Stores the complete block in the channel.
	 *
	 * @param channel
	 * @return Number of bytes written to the channel.
	 * @throws IOException
	 */
	//int storeFullTo(WritableByteChannel channel) throws IOException;
	
	/**
	 * Loads contents of this block from InputStream in.
	 * @param in
	 * @return Number of bytes read from the input stream.
	 * @throws IOException
	 */
	//int fromInputStream(InputStream in) throws IOException;
	
	/**
	 * Returns a ByteArrayInputStream that wraps around the byte[] containing the block in bytes. PrimaryNodeService
	 * calls this function to transfer data to the remote readers.Note that no guarantees are made about the concurrent
	 * modification of the block while the block is read from the stream.
	 */
	ByteString byteString();
	
	/**
	 * @return Size of the block in bytes when the complete block is serialized.
	 */
	int sizeWhenSerialized();
}
