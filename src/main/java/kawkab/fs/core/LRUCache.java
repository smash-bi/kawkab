package kawkab.fs.core;

import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.Map;

import kawkab.fs.commons.Constants;

public class LRUCache extends LinkedHashMap<String, CachedItem> {
	private Deque<CachedItem> evictedItems;
	
	public LRUCache(Deque<CachedItem> evictedItems){
		super(Constants.maxBlocksInCache+1, 1.1f, true);
		this.evictedItems = evictedItems;
	}
	
	@Override
	protected boolean removeEldestEntry(Map.Entry<String, CachedItem> eldest) {
		boolean remove = super.size() > Constants.maxBlocksInCache;
		if (!remove)
			return remove;
		
		CachedItem toEvict = eldest.getValue();
		
		
		//FIXME: What if the refCount of the toEvict is not zero??? We should not evict in that case, and search the next LRU where refCount is zero.
		
		//TODO: check if the refCount of toEvict is zero or not. If not, search and evict the next LRU item that has refCount == 0.
		if (remove){
			//addDirty(block);
			evictedItems.add(toEvict);
			if (toEvict.refCount() > 0) {
				System.out.println(" ============> Ref cnt " + toEvict.refCount() + " for " + toEvict.block().name());
			}
		}
		
		return remove;
	}
}