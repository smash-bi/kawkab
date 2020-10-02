package kawkab.fs.commons;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Base64;

public final class Commons {
	private static final int ibmapBlockSizeBytes;
	private static final int ibmapsPerMachine;
	private static final Configuration conf;
	
	static {
		try {
			conf = Configuration.instance();
			ibmapBlockSizeBytes = conf.ibmapBlockSizeBytes;
			ibmapsPerMachine = conf.ibmapsPerMachine;
		} catch (Exception | AssertionError e) {
			e.printStackTrace();
			throw e;
		}
	}
	
	public static String uuidToBase64String(long uuidHigh, long uuidLow){
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
     * Reads dstBuffer.remaining() bytes from the channel into the toBuffer.
     * 
     * @param channel Source channel
     * @param dstBuffer Destination buffer
     * @return Total number of bytes read from the channel
     * @throws IOException
     */
    public static int readFrom(ReadableByteChannel channel, ByteBuffer dstBuffer) throws IOException {
    	int readNow;
    	int totalRead = 0;

    	while(dstBuffer.remaining() > 0) {
    		readNow = channel.read(dstBuffer);
    		if (readNow == -1)
    			break;

    		totalRead += readNow;
    	}
    	
    	return totalRead;
    }
    
    /**
     * Writes srcBuffer.remaining() bytes from srcBuffer into dstChannel.
     * @param dstChannel
     * @param srcBuffer
     * @return The number of bytes written into the channel.
     * @throws IOException
     */
    public static int writeTo(WritableByteChannel dstChannel, ByteBuffer srcBuffer) throws IOException {
    	int bytesWritten = 0;
    	int size = srcBuffer.remaining();
    	while(bytesWritten < size) {
    		bytesWritten += dstChannel.write(srcBuffer);
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

    public static boolean onPrimaryNode(long inumber) {
    	return conf.thisNodeID == primaryWriterID(inumber);
	}
    
    public static int primaryWriterID(long inumber) {
		long inodeBlocksPerIbmap = ibmapBlockSizeBytes * Byte.SIZE;
		long ibmapNum = inumber / inodeBlocksPerIbmap;
		
		return ibmapOwner(ibmapNum);
	}
    
    public static int ibmapOwner(long ibmapNum) {
    	return (int)(ibmapNum / ibmapsPerMachine); //TODO: Get this number from ZooKeeper
    }

	public static synchronized void writeToFile(String lines, String outFile) {
		File file = new File(outFile).getParentFile();
		if (!file.exists()) {
			file.mkdirs();
		}

		try (BufferedWriter writer = new BufferedWriter(new FileWriter(outFile));) {
			writer.write(lines);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}

