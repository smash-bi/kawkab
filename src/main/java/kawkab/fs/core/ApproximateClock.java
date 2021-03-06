package kawkab.fs.core;

import java.util.concurrent.atomic.AtomicLong;

public class ApproximateClock implements Runnable {
	private static ApproximateClock instance;
	
	private final AtomicLong time;
	private final Thread timer;
	private volatile boolean working;
	
	private ApproximateClock() {
		working = true;
		time = new AtomicLong(System.currentTimeMillis());
		timer = new Thread(this);
		timer.setName("ApproximateClock");
		timer.setDaemon(true);
		timer.start();
	}
	
	public static synchronized ApproximateClock instance() {
		if (instance == null) {
			instance = new ApproximateClock();
		}
		
		return instance;
	}

	/**
	 * Returns an approximate current time in millis.
	 */
	public long currentTime() {
		return time.get();
	}

	@Override
	public void run() {
		while(working) {
			time.set(System.currentTimeMillis());
			try {
				Thread.sleep(1);
			} catch (InterruptedException e) {}
		}
	}
	
	public void shutdown() {
		working = false;
		try {
			timer.interrupt();
			timer.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
