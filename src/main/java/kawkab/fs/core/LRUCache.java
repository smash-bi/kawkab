package kawkab.fs.core;

import java.util.LinkedHashMap;
import java.util.Map.Entry;
import java.util.Queue;

import kawkab.fs.commons.Constants;

@SuppressWarnings("serial")
public class LRUCache extends LinkedHashMap<String, CachedItem> {
	private Queue<CachedItem> evictedItems;
	
	public LRUCache(Queue<CachedItem> evictedItems){
		super(Constants.maxBlocksInCache+1, 1.1f, true);
		this.evictedItems = evictedItems;
	}
	
	@Override
	protected boolean removeEldestEntry(Entry<String, CachedItem> eldest) {
		if (!(super.size() >= Constants.maxBlocksInCache))
			return false;
		
		CachedItem toEvict = eldest.getValue();
		
		//addDirty(block);
		if (toEvict.refCount() > 0) {
			super.replace(eldest.getKey(), eldest.getValue()); //FIXME: This is not a good approach.
			//System.out.println(" ============> Ref cnt " + toEvict.refCount() + " for " + toEvict.block().name());
			
			int count = 0;
			int size = super.size();
			for (Entry<String, CachedItem> entry : super.entrySet()) {
				count++;
				CachedItem cached = entry.getValue();
				if (cached.refCount() == 0) {
					if (count == size) {//FIXME: We just added the time. We should not remove this item.
						System.out.println("\t\t\t ---> Evicting just added block.");
					}
					super.remove(entry.getKey());
					evictedItems.add(cached);
					
					return false;
				}
			}
			
			//FIXME: What should we do in this situation? We don't have any block that can be evicted. All the blocks have ref-count > 0.
		}
		
		evictedItems.add(toEvict);
		
		return true;
	}
}