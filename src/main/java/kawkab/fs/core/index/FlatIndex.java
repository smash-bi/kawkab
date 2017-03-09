package kawkab.fs.core.index;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import kawkab.fs.commons.Constants;
import kawkab.fs.core.DataBlock;
import kawkab.fs.core.DataIndex;

public class FlatIndex {
	private final int dataBlockSize;
	private Map<Long, Long> timeToByteOffset;
	private Map<Long, DataBlock> byteToDataBlock;
	private List<DataIndex> dataIndex;
	private List<DataBlock> dataBlocks;
	private long blocksCnt;
	
	public FlatIndex(){
		timeToByteOffset = new ConcurrentHashMap<Long, Long>();
		byteToDataBlock = new ConcurrentHashMap<Long, DataBlock>();
		dataBlockSize = Constants.dataBlockSizeBytes;
		dataIndex = new ArrayList<DataIndex>();
		dataBlocks = new ArrayList<DataBlock>();
	}
	
	public DataBlock getByByte(long byteOffset){
		if (byteOffset < 0 || byteOffset > blocksCnt*dataBlockSize) {
			//TODO: throw exception InvalidOffsetException
			return null;
		}
		
		long blockID = byteOffset/dataBlockSize;
		return byteToDataBlock.get(blockID);
	}
	
	public DataBlock getByTime(long time){
		//Perform binary search
		
		return null;
	}
	
	public void add(DataIndex index, DataBlock block){
		dataBlocks.add(block);
		dataIndex.add(index);
		
		byteToDataBlock.put(index.offsetInFile(), block);
		timeToByteOffset.put(index.timestamp(), index.offsetInFile());
	}
}
