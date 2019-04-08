package kawkab.fs.core;

import java.util.concurrent.ConcurrentLinkedQueue;

import kawkab.fs.core.exceptions.KawkabException;

public class SegmentTimerQueue {
	private final ConcurrentLinkedQueue<SegmentTimer> buffer;
	private final Thread timerThread;
	private final Time time;
	private volatile boolean working = true;
	
	private static SegmentTimerQueue instance;
	
	private SegmentTimerQueue () {
		buffer = new ConcurrentLinkedQueue<SegmentTimer>();
		time = Time.instance();
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
	
	public void add(SegmentTimer timer) {
		if (timer.getAndSetInQueue(true))
			return;
		
		buffer.add(timer);
	}
	
	private void processSegments() {
		SegmentTimer next = null;
		
		while(working) {
			next = buffer.poll();
			
			if (next == null) {
				try {
					Thread.sleep(2); //2 is chosen randomly. It doesn't impact the performance, but a large value may result in slower expiry of the segments
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
		
		while((next = buffer.poll()) != null) {
			try {
				process(next);
			} catch (KawkabException e) {
				e.printStackTrace();
			}
		}
	}
	
	private void process(SegmentTimer timer) throws KawkabException {
		timer.getAndSetInQueue(false);
		
		long timeNow = time.currentTime();
		long ret = timer.tryExpire(timeNow);
		
		while (ret > 0) { //While (!EXPIRED && !DISABLED)
			try {
				Thread.sleep(ret);
			} catch (InterruptedException e) {}
			
			timeNow = time.currentTime();
			ret = timer.tryExpire(timeNow);
		}
		
		// if (ret == SegmentTimer.DISABLED || ret == SegmentTimer.ALREADY_EXPIRED)
		//	return;
		//assert ret == SegmentTimer.EXPIRED;
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
