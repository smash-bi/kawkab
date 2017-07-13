package kawkab.fs.core;

public interface BlockEvictionListener {
	public void beforeEviction(CachedItem cachedItem);
}
