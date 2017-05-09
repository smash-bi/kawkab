package kawkab.fs.commons;

import java.nio.ByteBuffer;
import java.util.Base64;

public class Commons {
	public static long maxDataSize(int indexLevel){
		return (long)Math.pow(Constants.numPointersInIndexBlock, indexLevel) * Constants.dataBlockSizeBytes;
	}
	
	public static long maxBlocksCount(int indexLevel){
		return (long)Math.pow(Constants.numPointersInIndexBlock, indexLevel);
	}
	
	public static String uuidToString(long uuidHigh, long uuidLow){
		byte[] id = new byte[16];
		ByteBuffer buffer = ByteBuffer.wrap(id);
		buffer.putLong(uuidHigh);
		buffer.putLong(uuidLow);
		buffer.clear();
		return Base64.getUrlEncoder().withoutPadding().encodeToString(id);
	}
}

