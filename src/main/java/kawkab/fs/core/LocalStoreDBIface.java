package kawkab.fs.core;

public interface LocalStoreDBIface {
	void put(BlockID id);
	boolean exists(BlockID id);
	String removeEntry(BlockID id);
	int size();
	void shutdown();
}
