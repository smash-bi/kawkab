package kawkab.fs.core;

import kawkab.fs.core.exceptions.KawkabException;

import java.util.concurrent.atomic.AtomicLong;

/**
 * This timer is used with BufferedSegment objects to expire them after a timeout.
 * The caller should follow the following semantics: <code>
	 // Later on before following the critical path associated with this timer
	 if (!timer.diableIfNotExpired()) {
	 timer = new SegmentTimer();
	 }
	 ...
	 // Perform mutually exclusive task
	 timer.update()
	 addInQueue(timer)
	 }
 * </code>
 *
 *
 * The worker that dequeues the timer can turn off the timer if the timer's time has expired. The worker calls the
 * tryExpire() function to turn off the timer. The timer is turned off only if it is not disabled or not already turned
 * off.
 *
 * Note that the disableIfNotExpired() and update() functions must duel. First create object, then disableIfNotExpired(),
 * update(), disableIfNotExpired(), update, and so on.
 *
 * The mState has only three states: (1) Disabled, (2) Expired, and (3) Valid. Expired is a terminal state. Initial
 * state is Disabled. States can transition as: Disabled -> Valid,  valid -> Disabled, valid -> Expired.
 *
 * I am not sure if we allow the following transitions: disabled -> disabled, valid -> valid. These look like safe and
 * enabled in the code, but the safety need to be verified. Transition expired -> expired is prevented in the code.
 *
 * A valud state means the mState contains the remaining amount of time in millis. Disabled means the timer has to
 * be renabled and the timer cannot be expired. Expired means that the timer was valid and the timer has now expired.
 * The timer cannot be disabled or made valid after it has been expired.
 */

public class SegmentTimer {
	public static final int TIMEOUT_MS = 5; //Randomly chosen, a small value should be sufficient as we want to batch back-to-back writes only

	private final AtomicLong mState; //State of the timer, which can be DISABLED, EXPIRED, ALREADY_EXPIRED, or last timestamp
	private final static Clock clock = Clock.instance();
	
	public final static int DISABLED = 0; // Must be zero
	public final static int EXPIRED  = -1; // Must be a negative value
	public final static int ALREADY_EXPIRED = -2; //Must be a negative value. This is never set in mState variable.
	
	public SegmentTimer() {
		mState = new AtomicLong(DISABLED);
	}
	
	/**
	 * Restarts the timer, which was newly created or previously disabled. The timer must be disabled before calling
	 * this update(). The timer is disabled by calling the disableIfNotExpired() function.
	 */
	public void update() {
		// The state is DISABLED at this point in all the cases. This is because this timer is either new or is disabled
		// before it is expired. If it is new, the worker thread does not have access to the timer. If it is disabled,
		// the worker thread does not update the state. The worker changes the state only if the read state is not
		// DISABLED and not EXPIRED.
		
		// We don't need to check the previous state because the append thread has previously set the state
		// to disabled. Moreover, the worker thread will not concurrently update the state variable.
		long prev = mState.getAndSet(clock.currentTime());
		
		//assert prev == DISABLED; // FIXME: Is it necessary???
		assert prev != EXPIRED; // FIXME: Is it necessary???
	}
	
	/**
	 * 
	 * @return false if the timer has already expired based on the previous timer value, otherwise returns true
	 */
	public boolean disableIfNotExpired() {
		long state;
		do {
			state = mState.get();
			
			//assert state != DISABLED; //FIXME: Should we allow calling disable multiple times???
			
			if (state == DISABLED)
				return true;
			
			if (state == EXPIRED) // If the state is expired once, no thread updates it to any other state
				return false;
		} while(!mState.compareAndSet(state, DISABLED));
		
		return true;
	}
	
	/**
	 * Atomically expires the timer if the timer goes beyond the defined timeout
	 * @param timeNow the current clock in millis
	 * 
	 * @return Negative value (EXPIRED_NOW or ALREADY_EXPIRED) if the timer has expired in the current function call
	 *         or if the timer has already expired in some previous function call,
	 *         0 (DISABLED) if the timer is disabled in this function call,
	 *         remaining clock of the timer in millis otherwise.
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
				return DISABLED;			// the timer again in the queue. Instead the writer thread has added again or will add again in the queue.
			
			long diff = timeNow - state;	// The valid state contains the timestamp. 
			
			if (diff <= 0) 			// The diff can be negative because the state may have been updated by the writer  
				return TIMEOUT_MS;	// thread since the caller read the clock. Conservatively return the timeout value
			
			if (diff < TIMEOUT_MS)
				return diff;
			
		} while(!mState.compareAndSet(state, EXPIRED)); // We may be looping here multiple times, but that is fine because the thread is not on the critical path

		return EXPIRED;
	}
	
	public boolean isDisabled() {
		return mState.get() == DISABLED;
	}
	
	public boolean isExpired() {
		long state = mState.get();
		return state == EXPIRED;
	}
}
