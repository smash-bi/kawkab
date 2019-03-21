package kawkab.fs.core;

import java.util.concurrent.ConcurrentLinkedQueue;

public class DSPool {
	private static DSPool instance;
	
	private ConcurrentLinkedQueue<DataSegment> pool;
	private final int capacity = 5000; //Assuming 1MB DS size, sustain 5000MB/s for 5 seconds, which is about 5GB.
	
	private DSPool() {
		System.out.println("Initializing DataSegments Pool. Creating " + capacity + " data segments.");
		pool = new ConcurrentLinkedQueue<DataSegment>();
		for (int i=0; i<capacity; i++) {
			pool.offer(new DataSegment(new DataSegmentID(0,0,0)));
		}
	}
	
	public static synchronized DSPool instance() {
		if (instance == null) {
			instance = new DSPool();
		}
		
		return instance;
	}
	
	public DataSegment acquire() {
		//TODO: Wait if the queue is empty
		DataSegment ds = pool.poll();
		
		if (ds == null) { //If we run out of DataSegments, wakeup the thread to create more DSes and return a new DS
			ds = new DataSegment(new DataSegmentID(0,0,0));
			//TODO: Wakeup the thread to create new DSes
		}
		
		return ds;
	}
	
	public void release(DataSegment ds) {
		//System.out.println("Released " + ds.id());
		pool.offer(ds);
		
		//TODO: If the current pool size is larger than the initial capcity for a timeout, wakeup the thread to destory
		// some DSes
	}
}
