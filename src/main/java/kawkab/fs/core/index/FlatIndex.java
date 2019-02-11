package kawkab.fs.core.index;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import kawkab.fs.commons.Configuration;
import kawkab.fs.core.DataSegment;
import kawkab.fs.core.DataIndex;

public class FlatIndex {
	private final int dataBlockSize;
	private Map<Long, Long> timeToByteOffset;
	private Map<Long, DataSegment> byteToDataBlock;
	private List<DataIndex> dataIndex;
	private List<DataSegment> dataSegments;
	private long blocksCnt;
	
	public FlatIndex(){
		timeToByteOffset = new ConcurrentHashMap<Long, Long>();
		byteToDataBlock = new ConcurrentHashMap<Long, DataSegment>();
		dataBlockSize = Configuration.instance().segmentSizeBytes;
		dataIndex = new ArrayList<DataIndex>();
		dataSegments = new ArrayList<DataSegment>();
	}
	
	public DataSegment getByByte(long byteOffset){
		if (byteOffset < 0 || byteOffset > blocksCnt*dataBlockSize) {
			//TODO: throw exception InvalidOffsetException
			return null;
		}
		
		long blockID = byteOffset/dataBlockSize;
		return byteToDataBlock.get(blockID);
	}
	
	public DataSegment getByTime(long time){
		//Perform binary search
		
		return null;
	}
	
	public void add(DataIndex index, DataSegment block){
		dataSegments.add(block);
		dataIndex.add(index);
		
		byteToDataBlock.put(index.offsetInFile(), block);
		timeToByteOffset.put(index.timestamp(), index.offsetInFile());
	}
}
