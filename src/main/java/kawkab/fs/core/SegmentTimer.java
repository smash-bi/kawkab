package kawkab.fs.core;

import kawkab.fs.core.exceptions.KawkabException;

import java.util.concurrent.atomic.AtomicLong;

public class SegmentTimer extends AbstractTransferItem{
	public static final int TIMEOUT_MS = 5; //Randomly chosen, a small value should be sufficient as we want to batch back-to-back writes only

	private final AtomicLong mState;
	private final Inode inode;
	private final DataSegmentID segId;
	private final static Clock clock = Clock.instance();
	
	public final static int DISABLED = 0; // Must be zero
	public final static int EXPIRED  = -1; // Must be a negative value
	public final static int ALREADY_EXPIRED = -2; //Must be a negative value. This is never set in mState variable.

	public SegmentTimer(DataSegmentID id, Inode inode) {
		segId = id;
		mState = new AtomicLong(DISABLED);
		this.inode = inode;
	}
	
	public void update() {
		// The state is DISABLED at this point in all the cases. This is because this timer is either new or is disabled
		// before it is expired. If it is new, the worker thread does not have access to the timer. If it is disabled,
		// the worker thread does not update the state. The worker changes the state only if the read state is not
		// DISABLED and not EXPIRED.
		
		// We don't need to check the previous state because the append thread has previously set the state
		// to disabled. Moreover, the worker thread will not concurrently update the state variable.
		long prev = mState.getAndSet(clock.currentTime());
		
		assert prev == DISABLED;
	}
	
	/**
	 * 
	 * @return false if the timer has already expired based on the previous timer value, otherwise returns true
	 */
	public boolean disableIfValid() {
		long state;
		do {
			state = mState.get();
			
			assert state != DISABLED;
			
			if (state == EXPIRED) // If the state is expired once, no thread updates it to any other state
				return false;
		} while(!mState.compareAndSet(state, DISABLED));
		
		return true;
	}
	
	/**
	 * Atomically expires the timer if the timer goes beyond the defined timeout
	 * @param timeNow the current clock in millis
	 * 
	 * @return EXPIRED_NOW (negative value) if the timer has expired in the current function call,
	 *         ALREADY_EXPIRED (negative value) if the timer has already expired in some previous function call
	 *         DISABLED (0 value) if the timer is disabled, 
	 *         otherwise the remaining clock of the timer in millis
	 */
	public long tryExpire(long timeNow) throws KawkabException {
		long state;
		do {
			state = mState.get();
			
			if (state == EXPIRED)		// This can happen when (1) the writer adds the timer in the queue, (2) the worker removes the item from the queue and then preemepts,  
				return ALREADY_EXPIRED;	// (3) the writer thread comes again, disables the timer, updates the timer and puts the timer again in the queue, 
										// (4) the worker awakes, finds the timer to be expired, and expires the timer. (5) the worker gets the timer again from the queue, which
										// is already expired
			
			if (state == DISABLED)	// If the state is disabled, return the caller. Note that the caller should not add 
				return 0;			// the timer again in the queue. Instead the writer thread has added again or will add again in the queue.
			
			long diff = timeNow - state;	// The valid state contains the timestamp. 
			
			if (diff <= 0) 			// The diff can be negative because the state may have been updated by the writer  
				return TIMEOUT_MS;	// thread since the caller read the wall clock. Conservatively return the timeout value
			
			if (diff < TIMEOUT_MS)
				return diff;
			
		} while(!mState.compareAndSet(state, EXPIRED)); // We may be looping here multiple times, but that is fine because the thread is not on the critical path

		inode.onTimerExpiry(this, segId);

		return EXPIRED;
	}
}
