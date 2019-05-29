package kawkab.fs.core;

/**
 * This class works in conjunction with TransferQueue.
 * TransferItems are used in the TransferQueue to share obejcts between a producer and a consumer. The only purpose
 * of the TransferQueue is to allow repeated transfers of the same object from one thread to another through a queue
 * while keeping the items in the queue unique.
 *
 * There is a race between the producer and the consumer when they concurrently call getAndSetInQueue and inTransferQueue.
 * However, the race is safe due to the ordering of the functions. The only requirement is that the object is not lost.
 * Either the object is already in the queue, or if it is not in the queue, the TransferQueue worker has the object.
 *
 *
 * The TransferQueue (TQ) and the TransferItems (TI) are used in the following way:
 * Producer has an object, which it updates. Then the producer calls the TQ.add(TI) function to transfer the object
 * to the consumer ... TBD
 */
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
