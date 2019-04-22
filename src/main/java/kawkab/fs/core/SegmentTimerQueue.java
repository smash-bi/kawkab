package kawkab.fs.core;

import java.util.concurrent.ConcurrentLinkedQueue;

import kawkab.fs.core.exceptions.KawkabException;

public class SegmentTimerQueue {
	private final TransferQueue<SegmentTimer> buffer;
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
	
	public void add(SegmentTimer timer, long inumber) {
		assert working;
		
		buffer.add(timer, inumber);
	}
	
	private void processSegments() {
		SegmentTimer next = null;
		
		while(working) {
			try {
				next = buffer.take();
			} catch (InterruptedException e) {
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
	
	private void process(SegmentTimer timer) throws KawkabException {
		long timeNow = clock.currentTime();
		long ret = timer.tryExpire(timeNow);
		
		while (ret > 0) { //While (!EXPIRED && !DISABLED)
			try {
				Thread.sleep(ret);
			} catch (InterruptedException e) {}
			
			timeNow = clock.currentTime();
			ret = timer.tryExpire(timeNow);
		}
		
		// if (ret == SegmentTimer.DISABLED || ret == SegmentTimer.ALREADY_EXPIRED)
		//	return;
		//assert ret == SegmentTimer.EXPIRED;
	}
	
	public void shutdown() {
		System.out.println("Closing SegmentTimerQueue");
		working = false;
		
		buffer.shutdown();
		
		timerThread.interrupt();
		try {
			timerThread.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
