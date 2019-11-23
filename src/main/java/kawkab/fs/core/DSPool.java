package kawkab.fs.core;

import java.io.PrintStream;
import java.util.concurrent.ConcurrentLinkedQueue;

public class DSPool {
	private ConcurrentLinkedQueue<DataSegment> pool;
	private final int capacity;
	private static final DataSegmentID tempID = new DataSegmentID(-1,-1,-1, -1);

	public DSPool(int capacity) {
		System.out.println("Initializing DataSegments pool of size " + capacity);
		pool = new ConcurrentLinkedQueue<>();
		for (int i=0; i<capacity; i++) {
			pool.offer(new DataSegment(tempID));
		}

		this.capacity = capacity;
	}
	
	public synchronized DataSegment acquire(DataSegmentID dsid) {
		//TODO: Wait if the queue is empty
		DataSegment ds = pool.poll();
		
		assert ds != null : "DSPool is empty";

		assert ds.id() == null || ds.id().equals(tempID);

		assert ds.dbgDspCnt == 0;

		assert ds.dbgAcq() == 0 : String.format("DS %s is ref by %s cnt %d while acq for %s\n", ds.dbgSig, ds.id(), ds.dbgAcq(), dsid);

		ds.dbgDspCnt++;

		ds.reInit(dsid);
		
		return ds;
	}
	
	public synchronized void release(DataSegment ds) {
		//System.out.println("Released " + ds.id());

		assert ds.dbgAcq() == 0 : " DS ref should be zero: " + ds.dbgAcq();

		ds.reset(null);

		ds.dbgDspCnt--;

		assert ds.id() == null;
		assert ds.dbgDspCnt == 0;

		pool.offer(ds);

		assert pool.size() <= capacity;

		//TODO: If the current pool size is larger than the initial capcity for a timeout, wakeup the thread to destory
		// some DSes
	}
	
	public int size() {
		return pool.size();
	}
}
