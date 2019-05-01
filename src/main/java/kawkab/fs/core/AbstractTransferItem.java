package kawkab.fs.core;

public class AbstractTransferItem {
	private volatile boolean inQueue = false;
	
	public long inQCount = 0; //For debug purposes
	public long inTries = 0; //For debug purposes
	
	/**
	 * This is not an atomic function. However, the race is safe when adding this object in <code>TransferQueue</code>.
	 *
	 * @param newVal
	 * @return
	 */
	public boolean getAndSetInQueue(boolean newVal) {
		if (newVal == inQueue)
			return newVal;
		
		inQueue = newVal;
		return !newVal;
	}
	
	public boolean inTransferQueue() {
		return inQueue;
	}
}
