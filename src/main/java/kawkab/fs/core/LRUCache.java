package kawkab.fs.core;

import java.util.LinkedHashMap;
import java.util.Map.Entry;

import kawkab.fs.commons.Configuration;

@SuppressWarnings("serial")
public final class LRUCache extends LinkedHashMap<BlockID, CachedItem> {
	private BlockEvictionListener evictListener;
	private final int maxBlocksInCache;
	
	public LRUCache(int maxBlocksInCache, BlockEvictionListener evictListener) {
		super(maxBlocksInCache + 1, 1.1f, true);
		this.maxBlocksInCache = maxBlocksInCache;
		this.evictListener = evictListener;
	}
	
	@Override
	protected boolean removeEldestEntry(Entry<BlockID, CachedItem> eldest) {
		if (super.size() < maxBlocksInCache)
			return false;
		
		CachedItem toEvict = eldest.getValue();
		
		//addDirty(block);
		if (toEvict.refCount() > 0) {
			super.replace(eldest.getKey(), eldest.getValue()); //FIXME: This is not a good approach.
			//System.out.println(" ============> Ref cnt " + toEvict.refCount() + " for " + toEvict.block().name());
			
			int count = 0;
			int size = super.size();
			for (Entry<BlockID, CachedItem> entry : super.entrySet()) {
				count++;
				CachedItem altToEvict = entry.getValue();
				if (altToEvict.refCount() == 0) {
					if (count == size) {//FIXME: We just added the time. We should not remove this item.
						System.out.println("\t\t\t ---> Evicting just added block.");
					}
					
					System.out.println("[LRUC] Cannot evict " + toEvict.block().id() + ", evicting: " + altToEvict.block().id());
					
					super.remove(entry.getKey());
					evictListener.beforeEviction(altToEvict);
					
					return false;
				}
			}
			
			//FIXME: What should we do in this situation? We don't have any block that can be evicted. All the blocks have ref-count > 0.
		}
		
		evictListener.beforeEviction(toEvict);
		
		return true;
	}
}