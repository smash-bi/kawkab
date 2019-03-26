package kawkab.fs.core;

import java.util.concurrent.atomic.AtomicBoolean;

public class SegmentTimer {
	private static final int timeoutMs = 1000; //Randomly chosen, a small value should be sufficient as we want to batch back-to-back writes only
	private final AtomicBoolean inQueue;
	private long timestamp;
	private final DataSegmentID segId;
	
	public SegmentTimer(DataSegmentID id) {
		segId = id;
		inQueue = new AtomicBoolean(false);
	}
	
	/**
	 * Sets the timer to be the current wall clock time.
	 * 
	 * @return true if the timer was expired based on the previous timer value, otherwise returns false
	 */
	public synchronized boolean update() {
		if (timestamp < 0)
			return false;
		
		timestamp = System.currentTimeMillis();
		
		return true;
	}
	
	public synchronized boolean disable() {
		if (timestamp < 0)
			return false;
		
		timestamp = 0;
		return true;
	}
	
	/**
	 * Atomically expires the timer if the timer goes beyond the defined timeout
	 * @param timeNow the current time in millis
	 * 
	 * @return -1 if the timer has expired, otherwise returns the remaining the lower bound millis until the timer expires
	 */
	public synchronized long expired(long timeNow) {
		if (timestamp < 0)
			return -1;
		
		if (timestamp == 0)
			return timeoutMs;
		
		long diff = timeNow - timestamp;
		
		if (diff >= timeoutMs) {
			timestamp = -1;
			return -1;
		}
		
		System.out.println("Expired " + segId);
		
		return diff;
	}
	
	public boolean getAndSetInQueue(boolean inQueue) {
		return this.inQueue.getAndSet(inQueue);
	}
	
	public DataSegmentID segmentID() {
		return segId;
	}
}
