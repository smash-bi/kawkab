package kawkab.fs.core;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

@SuppressWarnings("serial")
public final class LRUCache extends LinkedHashMap<BlockID, CachedItem> {
	private BlockEvictionListener evictListener;
	private final int maxBlocksInCache;
	private final int lowMark;
	private final int highMark;

	public LRUCache(int maxBlocksInCache, BlockEvictionListener evictListener) {
		super(2*maxBlocksInCache + 1, 0.75f, true);
		this.maxBlocksInCache = maxBlocksInCache;
		this.evictListener = evictListener;

		highMark = (int) (maxBlocksInCache*0.8);
		lowMark = (int) (maxBlocksInCache*0.5);
	}

	@Override
	protected boolean removeEldestEntry(Entry<BlockID, CachedItem> eldest) {
		int maxAllowed = maxBlocksInCache-1;
		if (size() < maxAllowed)
			return false;

		CachedItem toEvict = eldest.getValue();
		if (toEvict.refCount() == 0 && !toEvict.block().isLocalDirty()) {
			evictListener.beforeEviction(toEvict);
			return true;
		}

		/*Iterator<CachedItem> itr = super.values().iterator();
		while(itr.hasNext()) {
			toEvict = itr.next();
			if (toEvict.refCount() == 0) {
				evictListener.beforeEviction(toEvict);
				return false;
			}
		}*/

		//addDirty(block);
		// Move the non-evictable LRU item to the head
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

				//System.out.println("[LRUC] **** Cannot evict " + toEvict.block().id() + ", evicting: " + altToEvict.block().id());

				//super.remove(entry.getKey());
				evictListener.beforeEviction(altToEvict);

				return false;
			}
		}

		//FIXME: What should we do in this situation? We don't have any block that can be evicted. All the blocks have ref-count > 0.
		//evictListener.beforeEviction(toEvict);

		return false;
	}

	void bulkRemove(int toEvict) {
		//System.out.println("[LC] To evict = "+toEvict);

		CachedItem firstDirty = null;
		//int evicted = 0;
		//int skipped = 0;
		Iterator<Entry<BlockID, CachedItem>> itr = super.entrySet().iterator();
		//while(evicted < toEvict && itr.hasNext()) {
		for (int i=0; i<toEvict; i++) {
			if (!itr.hasNext())
				break;

			Entry<BlockID, CachedItem> e = itr.next();
			CachedItem ci = e.getValue();

			if (ci.refCount() != 0) {
				//skipped++;
				continue;
			}

			if (ci.block().isLocalDirty()) {
				//skipped++;
				// The most LRU dirty block
				if (firstDirty == null)
					firstDirty = ci;
				continue;
			}

			evictListener.onEvictBlock(ci.block());

			//evicted++;
			itr.remove();

			//assert dbgCI.equals(ci) : String.format("CachedItems not equal, eci.hc=%d, dbgCI.hc=%d, eci.id=%s, dbgCI.id=%s, eci.id.hc=%d, dbgCI.id.hc=%d\n",
			//		ci.hashCode(), dbgCI.hashCode(), ci.block().id(), dbgCI.block().id(), ci.block().id().hashCode(), dbgCI.block().id().hashCode());
		}

		// Can't evict a block that is dirty. However, if all the blocks are dirty or are referenced,
		// evict the most LRU dirty block
		/*if (evicted == 0 && firstDirty != null) {
			assert firstDirty.refCount() == 0 : String.format("[LC] Evicting a referenced block: %s", firstDirty.block().id());

			evictListener.beforeEviction(firstDirty);
			// Do not call the onEvictBlock here as we want to release the lock before waiting for localStore sync
			System.out.println("[LC] Evicted after sync = 1");
			return;
		}*/

		//if (evicted != toEvict)
		//	System.out.println("[LC] Evicted = "+evicted+", skipped="+skipped);
	}
}