package kawkab.fs.api;

public final class FileOptions {
	//private int blockSizeMB; //Our system should control the block size
	//private CachingOptions chacheOpts;
	//private ReplicationOptions repOpts;
	//private StorageOptions storageOpts;
	
	public FileOptions(){
		//blockSizeMB = Constants.defaultBlockSize;
	}
	
	public static FileOptions defaultOpts() {
		return new FileOptions();
	}
	
	/*public int blockSize(){
		return blockSizeMB;
	}*/
}
