package kawkab.fs.core.index;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import kawkab.fs.commons.Constants;
import kawkab.fs.core.DataSegment;
import kawkab.fs.core.DataIndex;
import kawkab.fs.core.exceptions.InvalidFileOffsetException;

public class MappedIndex {
	private final int dataBlockSize;
	private Map<Long, Long> timeToByteOffset;
	private Map<Long, DataSegment> byteToDataBlock;
	
	public MappedIndex(){
		timeToByteOffset = new ConcurrentHashMap<Long, Long>();
		byteToDataBlock = new ConcurrentHashMap<Long, DataSegment>();
		dataBlockSize = Constants.segmentSizeBytes;
	}
	
	public DataSegment getByByte(long byteOffset) throws InvalidFileOffsetException{
		long fileSize = byteToDataBlock.size()*dataBlockSize;
		
		if (byteOffset < 0 || byteOffset > fileSize) {
			throw new InvalidFileOffsetException(
					String.format("Byte %d is out of bounds from length ", byteOffset, fileSize));
		}
		
		long blockID = byteOffset/dataBlockSize;
		return byteToDataBlock.get(blockID);
	}
	
	public DataSegment getByByte(){
		return null;
	}
	
	public DataSegment getByTime(long time){
		//int numBlocks = byteToDataBlock.size();
		
		return null;
	}
	
	public void add(DataIndex index, DataSegment block){
		byteToDataBlock.put(index.offsetInFile(), block);
		timeToByteOffset.put(index.timestamp(), index.offsetInFile());
	}
}
