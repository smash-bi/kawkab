package kawkab.fs.api;

import kawkab.fs.api.indexing.Index;
import kawkab.fs.api.indexing.TimeIndex;

public final class FileOptions {
	//private int blockSizeMB; //Our system should control the block size
	private Index index;
	//private CachingOptions chacheOpts;
	//private ReplicationOptions repOpts;
	//private StorageOptions storageOpts;
	
	public FileOptions(){
		//blockSizeMB = Constants.defaultBlockSize;
		index = new TimeIndex();
	}
	
	public FileOptions setIndex(Index index){
		this.index = index;
		return this;
	}
	
	/*public FileOptions setBlockSize(int blockSizeMB){
		this.blockSizeMB = blockSizeMB;
		return this;
	}*/
	
	public Index index(){
		return index;
	}
	
	/*public int blockSize(){
		return blockSizeMB;
	}*/
}
