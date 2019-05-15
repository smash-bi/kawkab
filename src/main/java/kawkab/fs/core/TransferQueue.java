package kawkab.fs.core;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * A semi-blocking queue to transfer objects from one thread to another thread. The producer's add operations is non-blocking
 * and consumers take operations is blocking. The purpose of this queue is to allow idempotent add operations of the
 * same object in a scalable way with least number of CPU cycles.
 *
 * The expected use case is:
 * Producer: object -> update -> addInQueue
 * Consumer: removeFromQueue -> process object
 *
 * The producer and consumer need an external synchronization to agree on the updates. For example, the producer
 * updates the dirty count after each update while the consumer substracts the dirty count by the amount it has
 * processed.
 */
public class TransferQueue <T extends AbstractTransferItem> {
	private BlockingQueue<T> unifiedQueue;
	
	private long inQCountAgg = 0; //For debugging only
	private long inTriesAgg = 0; //For debugging only
	
	public TransferQueue() {
		unifiedQueue = new ArrayBlockingQueue(50000);
	}
	
	/**
	 * Add item at the tail of the queue
	 * @param item
	 */
	public void add(T item) {
		item.inTries++;
		if (item.getAndSetInQueue(true)) { // The item is already in the queue
			return;
		}
		
		item.inQCount++;
		
		unifiedQueue.add(item);
	}
	
	/**
	 * Removes the item at the head of the queue. If the queue is empty, the function blocks until an item becomes
	 * available in the queue.
	 *
	 * @return the head of this queue
	 */
	public T take() throws InterruptedException {
		T item = unifiedQueue.take();
		item.getAndSetInQueue(false);
		
		inTriesAgg += item.inTries;
		inQCountAgg += item.inQCount;
		item.inTries = 0;
		item.inQCount = 0;
		
		return item;
	}
	
	/**
	 * Retrieves and removes the head of this queue, or returns null if this queue is empty.
	 * @return the head of this queue, or null if this queue is empty
	 */
	public T poll() {
		T item = unifiedQueue.poll();
		if (item == null)
			return null;
		
		item.getAndSetInQueue(false);
		
		inTriesAgg += item.inTries;
		inQCountAgg += item.inQCount;
		item.inTries = 0;
		item.inQCount = 0;
		
		return item;
	}
	
	public int size() {
		return unifiedQueue.size();
	}
	
	public void shutdown() {
		System.out.printf("\t[TQ] Total tries = %d, Total added in Q = %d, ratio = %.2f%%\n", inTriesAgg, inQCountAgg, 100.0*inQCountAgg/inTriesAgg);
	}
}

