package kawkab.fs.core;

import java.util.concurrent.atomic.AtomicLong;

public class Time implements Runnable {
	private static Time instance;
	
	private final AtomicLong time;
	private final Thread timer;
	private volatile boolean working;
	
	private Time() {
		working = true;
		time = new AtomicLong(System.currentTimeMillis());
		timer = new Thread(this);
		timer.start();
	}
	
	public static synchronized Time instance() {
		if (instance == null) {
			instance = new Time();
		}
		
		return instance;
	}
	
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
