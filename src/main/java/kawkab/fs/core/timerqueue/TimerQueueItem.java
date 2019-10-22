package kawkab.fs.core.timerqueue;

import kawkab.fs.core.AbstractTransferItem;
import kawkab.fs.core.Clock;
import kawkab.fs.core.exceptions.KawkabException;

/**
 * A TimerQueueItem wraps an object and defers some work for later on the object. Each object has an associated timer
 * offset. The TimerQueue performs the deferreWork() only once in its lifetime that which happens only after timer expires.
 *
 * Either the user itself can disable the work and manually call the deferredWork() function, or
 * the TimerQueue will call the deferred work after the timer expires.
 *
 * The object should be updated only before it expires and when disabled. Therefore, the caller must first disable the
 * associated timer by calling the disableIfNotExpired() function and then update the object. If the timer cannot be
 * disabled because it has expired, the user can assume that the deferredWork will eventually be called and the user
 * should not reuse the same TimerQueueItem wrapper around the object.
 *
 * Note that the timer is not a real timer. It is just an offset in future. It is not guaranteed that deferrredWork()
 * will be called after a fixed delay. Instead, either the user disables the timer and calls deferredWork itself, or
 * the TimerQueue eventually calls the deferredWork() function after an unknown delay passed the timer offset.
 *
 * After disabling the timer, the caller should either call the enable function so that deferredWork() can be called by
 * the TimerQueue, or the caller should itself call the deferredWork() function.
 * Once the object is disabled, the TimerQueue does not calls the deferredWork() function until the timer is enabled
 * again by calling the enable() function.
 *
 * Note 1: The user must first disableIfNotExpired() the timer before updating the object. So within the custom
 * functions in the object that update the object, the first line of code should be to verify if the
 * timer is disabled by calling the verifyCancelled() function.
 *
 * Note 2: When a new item is created, the timer is in disabled state. Therefore, the timer must be enabled after
 * creating a new TimerQueueItem object.
 *
 * Note 3: This class uses kawkab.fs.core.Clock for timings, which is an approximate clock. The clock time may be
 * staled as compared to the wall clock. The granularity of the clock is in milliseconds.
 *
 * Usage example:
 * 		TimerQueueItem item ...
 *
 * 		item.update()
 * 		if (!item.freezeIfNotExpired())
 * 			item = new TimeQueueItem()
 * 		item.update()
 * 		item.unfreezeAndEnqueue()
 * 		...
 *
 * deferredWork and custom implementation example:
 * 		public void myUpdateFunction() {
 *     		verifyFrozen();
 *     		updates...
 * 		}
 */

public class TimerQueueItem<T> extends AbstractTransferItem {
	private final ItemTimer timer;
	private static final Clock clock = Clock.instance();
	
	private T item;
	private DeferredWorkReceiver<T> receiver;

	private int acquireCount;

	public TimerQueueItem(T item, DeferredWorkReceiver<T> receiver) {
		this.item = item;
		this.receiver = receiver;
		timer = new ItemTimer();
	}
	
	/**
	 * This function disables the timer object if it has not already expired. If successfully disabled, the TIimerQUeue
	 * does not call to the deferredWork() function. If the item has already expired, which means the TimerQueue is in
	 * the process of calling deferredWork(), the caller must create a new item instead of reusing this item.
	 *
	 * Note that the timer is not expired immediately after the fixed timeout has passed. Instead, a timer is disabled only when
	 * the TimerQueue starts the procedure to call the deferredWork function. However, it is guaranteed that the
	 * the TimerQueue will not expire the timer before the time offset given in the enable() function.
	 *
	 * @return true if this item can be used and the timer is successfully disabled. Otherwise, it returns false,
	 * in which case the caller should create a new item and throw away the existing item to ensure that deferredWork()
	 * is called only once.
	 */
	protected final boolean disableIfNotExpired() {
		return timer.disableIfNotExpired();
	}
	
	/**
	 * Unfreeze an already frozen item and enables the internal timer associated with the item. This funtion internally adds this item in a
	 * TimerQueue. The TimerQueue pops the items and calls the tryExpire() function.
	 * 
	 * It is an error to call enable() after the timer has expired. So the caller should first disableIfNotExpired()
	 * before calling this function.
	 *
	 * @param futureTimeInMillis Time offset in millis after which the deferredWork() function can be called.
	 */
	protected final void enable(final long futureTimeInMillis) {
		timer.update(futureTimeInMillis);
	}
	
	/**
	 * This function tries to change the state of the timer has expired. If the timer still has some remaining time,
	 * the function returns a positive remaining time in mills. Otherwise, if the item was not expired previously,
	 * this function expires the timer and returns ItemTimer.EXPIRED.
	 * If the item was already expired due to a previous function call the function doesn't call the deferredWork() function
	 * and return ItemTimer.ALREADY_DISABLED.
	 *
	 * @return the remaining time of the timer until the timer expires if the item is not already expired. Otherwise,
	 * it returns 0 if the item is freezed and cannot be expired, or a negative value indicating that the item was expired
	 * and the deferredWork was called either now or in a previous function call.
	 *
	 * @throws KawkabException
	 */
	protected final long tryExpire() throws KawkabException {
		return timer.tryExpire(clock.currentTime());
	}
	
	/**
	 * This function is called only once in the lifetime of the TimerQueueItem. The function is thread-safe.
	 *
	 * @throws KawkabException
	 */
	protected void deferredWork() {
		receiver.deferredWork(item);
		item = null;
	}
	
	public T getItem() {
		return item;
	}
	
	@Override
	public String toString() {
		if (item == null) {
			return "EXPIRED ITEM";
		}
		
		return item.toString(); //FIXME: This is not thread safe. This should only be used for debuggin purposes.
	}

	public int incrementAndGet() {
		return ++acquireCount;
	}

	public int decrementAndGet() {
		return --acquireCount;
	}

	public int count() {
		return acquireCount;
	}
}
