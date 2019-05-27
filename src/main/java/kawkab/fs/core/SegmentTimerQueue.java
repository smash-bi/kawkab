package kawkab.fs.core;

import kawkab.fs.core.exceptions.KawkabException;

/**
 * A FIFO queue to expire the segments and return them to the cache. This implementation potentially returns items
 * to the cache much slower than required in some cases. As a result, the cache have some items that are eligible
 * for eviction but the cache cannot evict them as their reference count is greater than 0. .
 */
public class SegmentTimerQueue {
<<<<<<< HEAD
	private TransferQueue<SegmentTimer> buffer;
	private final Thread processorThr;
=======
	//private final TransferQueue<SegmentTimer> buffer;
	TransferQueue<BufferedSegment> buffer;
	private final Thread timerThread;
>>>>>>> batching
	private final Clock clock;
	private volatile boolean working = true;
	
	private static SegmentTimerQueue instance;
	
	private SegmentTimerQueue () {
		buffer = new TransferQueue<>();
		clock = Clock.instance();
		processorThr = new Thread("SegmentTimerQueueThread") {
			public void run() {
				processSegments();
			}
		};
		
		processorThr.start();
	}
	
	public static synchronized SegmentTimerQueue instance() {
		if (instance == null)
			instance = new SegmentTimerQueue();
		
		return instance;
	}
	
<<<<<<< HEAD
	/**
	 * Adds the timer object in the queue. The timer object is associated with a DataSegment.
	 * @param timer
	 */
	public void add(SegmentTimer timer) {
=======
	public void add(BufferedSegment segment) {
>>>>>>> batching
		assert working;
		
		buffer.add(segment);
	}
	
	/**
	 * Takes items from the queue,
	 */
	private void processSegments() {
		BufferedSegment next = null;
		
		while(working) {
			next = buffer.poll();
			if (next == null) {
				try { //Sleeping because the buffer is non-blocking
					Thread.sleep(1); //1ms is chosen randomly. It doesn't impact the performance, but a large value may result in slower expiry of the segments
				} catch (InterruptedException e) {}
				continue;
			}
		
			try {
				process(next);
			} catch (KawkabException e) {
				e.printStackTrace();
				continue;
			}
		}
		
		// Some items may have been left in the queue during closing. We have to retrieve those items and process
		// them before exit.
		while((next = buffer.poll()) != null) {
			try {
				process(next);
			} catch (KawkabException e) {
				e.printStackTrace();
			}
		}
	}
	
<<<<<<< HEAD
	/**
	 * @param timer
	 * @throws KawkabException
	 */
	private void process(SegmentTimer timer) throws KawkabException {
=======
	private void process(BufferedSegment bseg) throws KawkabException {
>>>>>>> batching
		long timeNow = clock.currentTime();
		long ret = bseg.tryExpire(timeNow);
		
		while (ret > 0) { //While (!EXPIRED && !DISABLED), which implies VALID, which implies a positive value greater than zero
			try {
				Thread.sleep(ret);
			} catch (InterruptedException e) {}
			
			timeNow = clock.currentTime();
			ret = bseg.tryExpire(timeNow);
		}
<<<<<<< HEAD
=======
		
		if (ret == 0) // If the bseg is frozen by the appender
			return;
		
		// The timer in the bseg is now expired. So bseg cannot be updated and now it is safe to release the internal DS.
		bseg.release();
>>>>>>> batching
	}
	
	public void waitUntilEmpty() {
		int size;
		while((size = buffer.size()) > 0) {
			System.out.println("Waiting until STQ becomes empty, current size = " + size);
			try {
				Thread.sleep(1);
			} catch (InterruptedException e) {
			}
		}
	}
	
	public void shutdown() {
		System.out.println("Closing SegmentTimerQueue");
		working = false;
		
		processorThr.interrupt();
		try {
			processorThr.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		buffer.shutdown();
	}
}
