package kawkab.fs.core;

import java.nio.ByteBuffer;

import kawkab.fs.core.exceptions.InsufficientResourcesException;

public abstract class Block implements AutoCloseable {
	abstract boolean dirty();
	abstract void clearDirty();
	abstract void fromBuffer(ByteBuffer buffer) throws InsufficientResourcesException;
	abstract void toBuffer(ByteBuffer buffer) throws InsufficientResourcesException;
	abstract String name();
	abstract String localPath();
	abstract int blockSize();
	
	@Override
	public void close(){
		if (dirty()){
			Cache.instance().addDirty(this);
		}
	}
}