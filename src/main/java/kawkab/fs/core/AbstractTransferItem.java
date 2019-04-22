package kawkab.fs.core;

public abstract class AbstractTransferItem {
	private volatile boolean inQueue = false;
	
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
}
