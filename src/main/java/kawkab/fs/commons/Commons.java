package kawkab.fs.commons;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Base64;

public class Commons {
	public static String uuidToString(long uuidHigh, long uuidLow){
		byte[] id = new byte[16];
		ByteBuffer buffer = ByteBuffer.wrap(id);
		buffer.putLong(uuidHigh);
		buffer.putLong(uuidLow);
		buffer.clear();
		return Base64.getUrlEncoder().withoutPadding().encodeToString(id);
	}
	
	/**
	 * Copied from Agrona library.
	 * 
     * Create a directory if it doesn't already exist.
     *
     * @param directory        the directory which definitely exists after this method call.
     * @param descriptionLabel to associate with the directory for any exceptions.
     */
    public static void ensureDirectoryExists(final File directory, final String descriptionLabel)
    {
        if (!directory.exists())
        {
            if (!directory.mkdirs())
            {
                throw new IllegalArgumentException("could not create " + descriptionLabel + " directory: " + directory);
            }
        }
    }
    
    /**
     * Reads toBuffer.remaining() bytes from the channel into the toBuffer.
     * 
     * @param channel Read from the channel
     * @param toBuffer Read into the buffer
     * @param size Number of bytes to read
     * @return Total number of bytes read from the channel.
     * @throws IOException
     */
    public static int readFrom(ReadableByteChannel channel, ByteBuffer toBuffer) throws IOException {
    	int readNow = 0;
    	int totalRead = 0;
    	int size = toBuffer.remaining();
    	
    	while(readNow >= 0 && totalRead < size) {
    		readNow = channel.read(toBuffer);
    		totalRead += readNow;
    	}
    	
    	return totalRead;
    }
    
    /**
     * Writes fromBuffer.remaining() bytes from fromBuffer into toChannel.
     * @param toChannel
     * @param fromBuffer
     * @param size
     * @return The number of bytes written into the channel.
     * @throws IOException
     */
    public static int writeTo(WritableByteChannel toChannel, ByteBuffer fromBuffer) throws IOException {
    	int bytesWritten = 0;
    	int size = fromBuffer.remaining();
    	while(bytesWritten < size) {
    		bytesWritten += toChannel.write(fromBuffer);
    	}
    	
    	return bytesWritten;
    }
    
    public static byte[] longToBytes(long longNum) {
        byte[] result = new byte[Long.BYTES];
        for (int i = Long.BYTES-1; i >= 0; i--) {
            result[i] = (byte)(longNum & 0xFF);
            longNum >>= Byte.SIZE;
        }
        return result;
    }

    public static long bytesToLong(byte[] b) {
        long result = 0;
        for (int i = 0; i < Long.BYTES; i++) {
            result <<= Byte.SIZE;
            result |= (b[i] & 0xFF);
        }
        
        return result;
    }
    
    public int myid() {
    	return Constants.thisNodeID; //FIXME: Get the id of the current node in a proper way. 
    }
    
    public int nodesCount() {
    	return Constants.nodesInSystem;
    }
    
    public static int primaryWriterID(long inumber) {
		long inodeBlocksPerIbmap = Constants.ibmapBlockSizeBytes * Byte.SIZE;
		long ibmapNum = inumber / inodeBlocksPerIbmap;
		
		return ibmapOwner(ibmapNum);
	}
    
    public static int ibmapOwner(long ibmapNum) {
    	return (int)(ibmapNum / Constants.ibmapsPerMachine); //TODO: Get this number from ZooKeeper
    }
}

