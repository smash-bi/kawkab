package kawkab.fs.core;

import kawkab.fs.core.exceptions.KawkabException;

public class SegmentTimerQueue {
	//private final TransferQueue<SegmentTimer> buffer;
	TransferQueue<BufferedSegment> buffer;
	private final Thread timerThread;
	private final Clock clock;
	private volatile boolean working = true;
	
	private static SegmentTimerQueue instance;
	
	private SegmentTimerQueue () {
		buffer = new TransferQueue<>();
		clock = Clock.instance();
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
	
	public void add(BufferedSegment segment) {
		assert working;
		
		buffer.add(segment);
	}
	
	private void processSegments() {
		BufferedSegment next = null;
		
		while(working) {
			next = buffer.poll();
			if (next == null) {
				try {
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
	
	private void process(BufferedSegment bseg) throws KawkabException {
		long timeNow = clock.currentTime();
		long ret = bseg.tryExpire(timeNow);
		
		while (ret > 0) { //While (!EXPIRED && !DISABLED)
			try {
				Thread.sleep(ret);
			} catch (InterruptedException e) {}
			
			timeNow = clock.currentTime();
			ret = bseg.tryExpire(timeNow);
		}
		
		// The timer in the bseg is now expired. So bseg cannot be updated and now it is safe to release the internal DS.
		bseg.release();
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
		
		timerThread.interrupt();
		try {
			timerThread.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		buffer.shutdown();
	}
}
