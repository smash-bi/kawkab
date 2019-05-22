package kawkab.fs.core;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

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
public class TransferPriorityQueue<T extends AbstractTransferItem> {
	private ConcurrentHashMap<Long, ConcurrentLinkedQueue<T>> queuesMap;
	private BlockingQueue<T> unifiedQueue;
	private Thread conveyor;
	private volatile boolean working = true;
	
	private long inQCountAgg = 0; //For debugging only
	private long inTriesAgg = 0; //For debugging only
	
	public TransferPriorityQueue() {
		queuesMap = new ConcurrentHashMap<>();
		unifiedQueue = new LinkedBlockingQueue<>();
		conveyor = new Thread("TransferQueueTransferrer"){
			@Override
			public void run() {
				transferItems();
			}
		};
		conveyor.start();
	}
	
	private void transferItems() {
		while(working) {
			boolean empty = true;
			for (ConcurrentLinkedQueue<T> queue : queuesMap.values()) { //Iterate over all of the queues
				for (T item : queue) {
					unifiedQueue.add(item); //Add items in a unified blocking queue
					empty = false;
				}
			}
			
			if (empty) {
				try {
					Thread.sleep(1); //Sleep for 1 ms as there was no item
				} catch (InterruptedException e) {
					continue;
				}
			}
		}
		
		// Transfer any remaining items that were missed before
		for (ConcurrentLinkedQueue<T> queue : queuesMap.values()) {
			for (T item : queue) {
				unifiedQueue.add(item);
			}
		}
	}
	
	/**
	 * Add item at the tail of the queue
	 * @param item
	 * @param id
	 */
	public void add(T item, long id) {
		item.inTries++;
		if (item.getAndSetInQueue(true)) { // The item is already in the queue
			return;
		}
		
		item.inQCount++;
		
		ConcurrentLinkedQueue<T> q = queuesMap.get(id);
		
		if (q == null) {
			q = new ConcurrentLinkedQueue<>();
			queuesMap.putIfAbsent(id, q);
		}
		
		q.add(item);
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
	
	public void shutdown() {
		System.out.println("Closing TransferQueue");
		working = false;
		
		conveyor.interrupt();
		try {
			conveyor.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		System.out.printf("\t[TPQ] Total tries = %d, Total added in Q = %d, ratio = %.2f%%\n", inTriesAgg, inQCountAgg, 100.0*inQCountAgg/inTriesAgg);
	}
}
