package kawkab.fs.core;

public class BlockLocation {
	private enum PersistenceState {
		LOCAL_MEMORY, LOCAL_SSD, LOCAL_DISK, REMOTE_MEMORY, REMOTE_SSD, EB3, S3
	};
	
	private String onDiskLocation;
	private PersistenceState state;
}
