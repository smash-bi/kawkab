package kawkab.fs.core;

public abstract class AbstractTransferItem {
	private volatile boolean inQueue = false;
	
	public long inQCount = 0; //For debug purposes
	public long inTries = 0; //For debug purposes
	
	/**
	 * This is not an atomic function. However, the race is safe when adding this object in <code>TransferQueue</code>.
	 *
	 * The implementation internally keeps a boolean variable. Only flips the value of the internal variable if it is
	 * different than <code>newVal</code>. Otherwise, it does not change anything and returns <code>newValue</code>
	 *
	 * @param newVal true marks the object to be in a TransferQueue, false marks the object as not in a TransferQueue
	 * @return
	 */
	public final boolean getAndSetInQueue(boolean newVal) {
		if (newVal == inQueue)
			return newVal;
		
		inQueue = newVal;
		return !newVal;
	}
	
	/**
	 * @return Whether this object is in a TransferQueue or not
	 */
	public final boolean inTransferQueue() {
		return inQueue;
	}
}
