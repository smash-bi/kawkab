package kawkab.fs.core;

import java.util.concurrent.ConcurrentLinkedQueue;

public class DSPool {
	private ConcurrentLinkedQueue<DataSegment> pool;
	
	public DSPool(int capacity) {
		System.out.println("Initializing DataSegments pool of size " + capacity);
		pool = new ConcurrentLinkedQueue<>();
		for (int i=0; i<capacity; i++) {
			pool.offer(new DataSegment(new DataSegmentID(0,0,0)));
		}
	}
	
	public DataSegment acquire(DataSegmentID dsid) {
		//TODO: Wait if the queue is empty
		DataSegment ds = pool.poll();
		
		assert ds != null;
		
		ds.reInit(dsid);
		
		return ds;
	}
	
	public void release(DataSegment ds) {
		//System.out.println("Released " + ds.id());
		pool.offer(ds);
		
		//TODO: If the current pool size is larger than the initial capcity for a timeout, wakeup the thread to destory
		// some DSes
	}
	
	public int size() {
		return pool.size();
	}
}
