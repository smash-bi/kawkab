package kawkab.fs.core;

import kawkab.fs.core.timerqueue.DeferredWorkReceiver;
import kawkab.fs.core.timerqueue.TimerQueue;
import kawkab.fs.core.timerqueue.TimerQueueIface;
import kawkab.fs.core.timerqueue.TimerQueueItem;

import java.io.File;
import java.util.concurrent.Semaphore;

import static kawkab.fs.core.LocalEvictQueue.EvictItem;

public class LocalEvictQueue implements DeferredWorkReceiver<EvictItem> {
	private TimerQueueIface timerQ;
	private final static int EVICT_ITEM_MS = 30000;
	private ApproximateClock clock = ApproximateClock.instance();
	private LocalStoreDB lsd;
	private Semaphore storePermits;

	public LocalEvictQueue(String name, LocalStoreDB lsd, Semaphore permits) {
		timerQ = new TimerQueue(name);
		this.lsd = lsd;
		this.storePermits = permits;
	}

	public void evict(BlockID id) {
		TimerQueueItem<EvictItem> item = new TimerQueueItem<>(new EvictItem(id), this);
		int toWaitMS = EVICT_ITEM_MS;
		if (storePermits.availablePermits() < 1000) {
			toWaitMS = 0;
		}
		timerQ.enableAndAdd(item, clock.currentTime()+toWaitMS);
	}

	public void shutdown() {
		timerQ.shutdown();
	}

	@Override
	public void deferredWork(EvictItem item) {
		BlockID id = item.id;

		if (lsd.removeEntry(id) == null) {
			return;
		}

		File file = new File(id.localPath());
		if (!file.delete()) {
			System.err.println("[LEQ] Unable to delete the file: " + file.getAbsolutePath());
		}

		//block.unsetInLocalStore();

		//int mapSize = storedFilesMap.size();
		//int permits = storePermits.availablePermits();
		//System.out.println("\t\t\t\t\t\t Evict: Permits: " + permits + ", map: " + mapSize);

		storePermits.release();
	}

	public static class EvictItem extends AbstractTransferItem {
		BlockID id;
		public EvictItem(BlockID id) {
			this.id = id;
		}
	}
}
