package kawkab.fs.core.index;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import kawkab.fs.commons.Constants;
import kawkab.fs.core.DataBlock;
import kawkab.fs.core.DataIndex;
import kawkab.fs.core.exceptions.InvalidFileOffsetException;

public class MappedIndex {
	private final int dataBlockSize;
	private Map<Long, Long> timeToByteOffset;
	private Map<Long, DataBlock> byteToDataBlock;
	
	public MappedIndex(){
		timeToByteOffset = new ConcurrentHashMap<Long, Long>();
		byteToDataBlock = new ConcurrentHashMap<Long, DataBlock>();
		dataBlockSize = Constants.dataBlockSizeBytes;
	}
	
	public DataBlock getByByte(long byteOffset) throws InvalidFileOffsetException{
		long fileSize = byteToDataBlock.size()*dataBlockSize;
		
		if (byteOffset < 0 || byteOffset > fileSize) {
			throw new InvalidFileOffsetException(
					String.format("Byte %d is out of bounds from length ", byteOffset, fileSize));
		}
		
		long blockID = byteOffset/dataBlockSize;
		return byteToDataBlock.get(blockID);
	}
	
	public DataBlock getByByte(){
		return null;
	}
	
	public DataBlock getByTime(long time){
		//int numBlocks = byteToDataBlock.size();
		
		return null;
	}
	
	public void add(DataIndex index, DataBlock block){
		byteToDataBlock.put(index.offsetInFile(), block);
		timeToByteOffset.put(index.timestamp(), index.offsetInFile());
	}
}
