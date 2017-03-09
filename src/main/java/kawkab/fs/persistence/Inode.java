package kawkab.fs.persistence;

import java.nio.ByteBuffer;

import kawkab.fs.commons.Constants;

public class Inode {
	private long uuidLow;
	private long uuidHigh;
	private long blocksCount;
	private long fileSize;
	
	private int directBlocksCreated;
	private long directBlockUuidLow[];
	private long directBlockUuidHigh[];
	
	private long indirectBlockUuidLow;
	private long indirectBlockUuidHigh;
	
	private long doubleIndirectBlockUuidLow;
	private long doubleIndirectBlockUuidHigh;
	
	private long tripleIndirectBlockUuidLow;
	private long tripleIndirectBlockUuidHigh;
	
	private boolean dirty;
	
	public Inode(){}
	
	public Inode(long uuidLow, long uuidHigh){
		this.uuidLow = uuidLow;
		this.uuidHigh = uuidHigh;
	}
	
	public int toBuffer(ByteBuffer buffer){
		int initPosition = buffer.position();
		buffer.putLong(uuidLow);
		buffer.putLong(uuidHigh);
		
		buffer.putLong(blocksCount);
		buffer.putLong(fileSize);
		buffer.putInt(directBlocksCreated);
		
		for(int i=0; i<directBlockUuidLow.length; i++){
			buffer.putLong(directBlockUuidLow[i]);
			buffer.putLong(directBlockUuidHigh[i]);
		}
		
		buffer.putLong(indirectBlockUuidLow);
		buffer.putLong(indirectBlockUuidHigh);
		
		buffer.putLong(doubleIndirectBlockUuidLow);
		buffer.putLong(doubleIndirectBlockUuidHigh);
		
		buffer.putLong(tripleIndirectBlockUuidLow);
		buffer.putLong(tripleIndirectBlockUuidHigh);
		
		int dataLength = buffer.position() - initPosition;
		int padLength = Constants.inodeSizeBytes - dataLength;
		
		byte[] padding = new byte[padLength];
		buffer.put(padding);
		
		return dataLength + padLength;
	}
	
	public int fromBuffer(ByteBuffer buffer){
		if (buffer.remaining() < Constants.inodeSizeBytes)
			return 0;
		
		int initPosition = buffer.position();
		
		uuidLow = buffer.getLong();
		uuidHigh = buffer.getLong();
		
		blocksCount = buffer.getLong();
		fileSize = buffer.getLong();
		directBlocksCreated = buffer.getInt();
		
		directBlockUuidLow = new long[Constants.maxDirectBlocks];
		directBlockUuidHigh = new long[Constants.maxDirectBlocks];
		for (int i=0; i<Constants.maxDirectBlocks; i++){
			directBlockUuidLow[i] = buffer.getLong();
			directBlockUuidHigh[i] = buffer.getLong();
		}
		
		indirectBlockUuidLow = buffer.getLong();
		indirectBlockUuidHigh = buffer.getLong();
		
		doubleIndirectBlockUuidLow = buffer.getLong();
		doubleIndirectBlockUuidHigh = buffer.getLong();
		
		tripleIndirectBlockUuidLow = buffer.getLong();
		tripleIndirectBlockUuidHigh = buffer.getLong();
		
		int dataLength = buffer.position() - initPosition;
		int padLength = Constants.inodeSizeBytes - dataLength;
		
		byte[] padding = new byte[padLength];
		buffer.get(padding);
		
		return dataLength + padLength;
	}
	
	public boolean dirty(){
		return dirty;
	}
	
	public void clear(){
		dirty = false;
	}
}
