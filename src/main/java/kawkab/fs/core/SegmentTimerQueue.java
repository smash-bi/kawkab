package kawkab.fs.core;

import java.util.concurrent.ConcurrentLinkedQueue;

import kawkab.fs.core.exceptions.KawkabException;

public class SegmentTimerQueue {
	private final ConcurrentLinkedQueue<SegmentTimer> queue;
	private final Cache cache;
	private final Thread timerThread;
	private volatile boolean working = true;
	
	private static SegmentTimerQueue instance;
	
	public SegmentTimerQueue () {
		queue = new ConcurrentLinkedQueue<SegmentTimer>();
		cache = Cache.instance();
		timerThread = new Thread("SegmentTimerQueueThread") {
			public void run() {
				processSegments();
			}
		};
		
		timerThread.start();
	}
	
	public static synchronized SegmentTimerQueue instance() {
		if (instance == null)
			instance = new SegmentTimerQueue();
		
		return instance;
	}
	
	public void submit(SegmentTimer timer) {
		if (timer.getAndSetInQueue(true))
			return;
		
		queue.add(timer);
	}
	
	private void processSegments() {
		while(working) {
			SegmentTimer next = queue.poll();
			if (next == null) {
				try {
					Thread.sleep(1);
				} catch (InterruptedException e) {}
				continue;
			}
			
			try {
				process(next);
			} catch (KawkabException e) {
				e.printStackTrace();
			}
		}
		
		SegmentTimer next = null;
		while((next = queue.poll()) != null) {
			try {
				process(next);
			} catch (KawkabException e) {
				e.printStackTrace();
			}
		}
	}
	
	private void process(SegmentTimer timer) throws KawkabException {
		timer.getAndSetInQueue(false);
		
		long timeNow = System.currentTimeMillis();
		
		long ret = timer.expired(timeNow);
		
		if (ret > 0) {
			try {
				Thread.sleep(ret);
			} catch (InterruptedException e) {}
			
			timeNow = System.currentTimeMillis();
			ret = timer.expired(timeNow);
		}
		
		if (ret <= 0) {
			cache.releaseBlock(timer.segmentID());
			return;
		}
			
		timer.getAndSetInQueue(true);
	}
	
	public void shutdown() {
		System.out.println("Closing SegmentTimerQueue");
		working = false;
		
		timerThread.interrupt();
		try {
			timerThread.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
