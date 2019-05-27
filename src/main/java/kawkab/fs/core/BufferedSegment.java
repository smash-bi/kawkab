package kawkab.fs.core;

import kawkab.fs.core.exceptions.KawkabException;

import java.io.IOException;

/**
 * Call release() only after calling freeze()
 *
 * Once you have called release(), the same BufferedSegment object cannot be reused to append the DS or for another DS.
 */
public class BufferedSegment extends AbstractTransferItem {
	private DataSegmentID dsid;
	private DataSegment ds;
	private final SegmentTimer timer;
	
	private static final Cache cache = Cache.instance();
	private static final SegmentTimerQueue timerQ = SegmentTimerQueue.instance();
	
	public BufferedSegment(DataSegmentID dsid, long fileSize) throws IOException, KawkabException {
		this.dsid = dsid;
		
		ds = (DataSegment) cache.acquireBlock(dsid);
		ds.initForAppend(fileSize);
		
		timer = new SegmentTimer();
	}
	
	/**
	 * @param data Data to be appended
	 * @param offset  Offset in data
	 * @param length  Number of bytes to append: data[offset] to data[offset+length-1] inclusive.
	 * @param offsetInFile
	 * @return number of bytes appended starting from the offset
	 * @throws IOException
	 */
	public int append(byte[] data, int offset, int length, long offsetInFile) throws IOException {
		assert timer.isDisabled() : "[BS] Error: Illegal state: you must first call freeze() before append.";
		
		return ds.append(data, offset, length, offsetInFile);
	}
	
	/**
	 * This function freezes the acquired segment so that it cannot be returned back to the cache.
	 *
	 * @return true if this segment can be reused. Otherwise, returns false, in which case the must create a new BufferedSegment object.
	 */
	public boolean freeze() {
		return timer.disableIfNotExpired();
	}
	
	public void unfreeze() {
		assert timer.isDisabled() : "[BS] Illegal state in unfreeze; timer is not disabled"; //FIXME: WHY?? Is it a mandatory error? What will go wrong if we allow unfreeze() an already unfreeze() BS???
		
		timer.update();
		timerQ.add(this);
	}
	
	public synchronized void release() throws KawkabException { //synchronized between the appender and the segmentTimerQueuue to release the DS
		// This function does not need to be synchronized with any other function because release() is only allowed to be called
		// after freeze() or after reminingTime() functions.
		
		if (ds == null)
			return; // The DS has been already released through this release() function call
		
		assert timer.isDisabled() || timer.isExpired() : "Invalid timer state: " + timer.state();
		
		ds = null;
		cache.releaseBlock(dsid);
		dsid = null;
		//timer = null;	// Don't add back in the queue as the timer will not be reused again. The segmentTimerQueue
						// thread will just throw way the timer because it is disabled.
	}
	
	/**
	 * @param timeNow Current time in millis
	 *
	 * @return if the timer is valid, it returns the remaining number of millis in the timer until it is expired.
	 * A valid timer has some time remaining until it is expired.
	 * Otherwise, if the timer is not valid, it returns 0 if the segment is freezed, or returns a negative value if the timer
	 * has expired.
	 *
	 * @throws KawkabException
	 */
	public long tryExpire(long timeNow) throws KawkabException {
		return timer.tryExpire(timeNow);
	}
	
	public boolean isFull() {
		return ds.isFull();
	}
}
