package kawkab.fs.core;

import java.util.concurrent.ConcurrentLinkedQueue;

public class DSPool {
	private ConcurrentLinkedQueue<DataSegment> pool;
	//private static final DataSegmentID tempID = new DataSegmentID(-1,-1,-1, -1);

	public DSPool(int capacity) {
		System.out.println("Initializing DataSegments pool of size " + capacity);
		pool = new ConcurrentLinkedQueue<>();
		for (int i=0; i<capacity; i++) {
			pool.offer(new DataSegment(null));
		}
	}
	
	public DataSegment acquire(DataSegmentID dsid) {
		//TODO: Wait if the queue is empty
		DataSegment ds = pool.poll();
		
		assert ds != null : "DSPool is empty";

		ds.reInit(dsid);
		
		return ds;
	}
	
	public void release(DataSegment ds) {
		//System.out.println("Released " + ds.id());

		ds.reset(null);

		pool.offer(ds);
	}
}
