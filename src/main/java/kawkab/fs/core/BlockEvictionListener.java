package kawkab.fs.core;

public interface BlockEvictionListener {
	/**
	 * This function is called when the cachedItem is being evicted from the cache.
	 */
	void beforeEviction(CachedItem cachedItem);
}
