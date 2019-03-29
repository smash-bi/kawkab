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
	 * @return false if the timer has already expired based on the previous timer value, otherwise returns true
	 */
	public synchronized boolean update() {
		assert timestamp >= 0;
		
		timestamp = System.currentTimeMillis();
		
		return true;
	}
	
	/**
	 * 
	 * @return false if the timer has already expired based on the previous timer value, otherwise returns true
	 */
	public synchronized boolean disable() {
		if (timestamp < 0) // if the timer has expired
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
		assert timestamp >= 0;	// The timestamp should never be negative because it is only set to -1 in this function. 
								// Morevoer, the worker should not add this timer back in the queue as it has expired. 
								// The appender will not add an expired timer in the queue
		
		if (timestamp == 0) // if the timer is disabled
			return timeoutMs;
		
		long diff = timeNow - timestamp;
		
		if (diff >= timeoutMs) { // if the timer has expired
			timestamp = -1;
			return -1;
		}
		
		if (diff <= 0) { // This happens if the writer and the worker contend for lock, and the worker calls this function after the writer calls the update function
			return timeoutMs;
		}
		
		return timeoutMs - diff;
	}
	
	/**
	 * Atomically sets the new value and returns the previous value
	 */
	public boolean getAndSetInQueue(boolean inQueue) {
		return this.inQueue.getAndSet(inQueue);
	}
	
	public DataSegmentID segmentID() {
		return segId;
	}
}
