package kawkab.fs.core;

import kawkab.fs.utils.LatHistogram;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("serial")
public final class LRUCache extends LinkedHashMap<BlockID, CachedItem> {
	private BlockEvictionListener evictListener;
	private final int maxBlocksInCache;
	private LatHistogram evLog;
	private int maxTimeMS = 60000;
	private ApproximateClock clock = ApproximateClock.instance();
	private final long stTime = System.currentTimeMillis();
	private final int highMark;
	private boolean evicting = false;

	public LRUCache(int maxBlocksInCache, BlockEvictionListener evictListener) {
		//super((int)((maxBlocksInCache/0.75)+1), 0.75f, true);
		super(maxBlocksInCache*2, 0.75f, true);
		this.maxBlocksInCache = maxBlocksInCache;
		this.evictListener = evictListener;
		evLog = new LatHistogram(TimeUnit.MICROSECONDS, "EvictLog", 100, 100000);
		highMark = (int)(0.95*maxBlocksInCache);
	}

	@Override
	protected boolean removeEldestEntry(Entry<BlockID, CachedItem> eldest) {
		if (size() < highMark)
			return false;

		CachedItem toEvict = eldest.getValue();
		if (toEvict.refCount() > 0 || toEvict.block().isLocalDirty())
			return false;

		//if (clock.currentTime() - toEvict.accessTime() < maxTimeMS)
		//	return false;

		evictListener.onEvictBlock(toEvict.block());

		return true;
	}

	/*@Override
	protected boolean removeEldestEntry(Entry<BlockID, CachedItem> eldest) {
		int maxAllowed = maxBlocksInCache-1;
		if (size() < maxAllowed)
			return false;

		CachedItem toEvict = eldest.getValue();
		if (toEvict.refCount() == 0 && !toEvict.block().isLocalDirty()) {
			evictListener.beforeEviction(toEvict);
			return true;
		}

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
	}*/

	void bulkRemove(int toEvict) {
		//System.out.println("[LC] To evict = "+toEvict);

		//int evicted = 0;
		//int skipped = 0;
		evLog.start();
		Iterator<Entry<BlockID, CachedItem>> itr = super.entrySet().iterator();
		//while(evicted < toEvict && itr.hasNext()) {
		for (int i=0; i<toEvict; i++) {
			if (!itr.hasNext())
				break;

			Entry<BlockID, CachedItem> e = itr.next();
			CachedItem ci = e.getValue();

			Block block = ci.block();
			if (ci.refCount() != 0 || block.isLocalDirty()) {
				//skipped++;
				continue;
			}

			evictListener.onEvictBlock(block);

			itr.remove();
			//evicted++;

			//assert dbgCI.equals(ci) : String.format("CachedItems not equal, eci.hc=%d, dbgCI.hc=%d, eci.id=%s, dbgCI.id=%s, eci.id.hc=%d, dbgCI.id.hc=%d\n",
			//		ci.hashCode(), dbgCI.hashCode(), ci.block().id(), dbgCI.block().id(), ci.block().id().hashCode(), dbgCI.block().id().hashCode());
		}
		evLog.end();

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

	public String getStats() {
		return evLog.getStats();
	}
}