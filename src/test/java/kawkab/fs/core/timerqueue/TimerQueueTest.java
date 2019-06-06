package kawkab.fs.core.timerqueue;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Random;

public class TimerQueueTest {
	public static void main(String args[]) {
		TimerQueueTest test = new TimerQueueTest();
		
		test.interfaceTest();
		test.bufferedUseTest();
	}
	
	public void interfaceTest() {
		DeferredWorkReceiver<MutableObject> listener = item -> item.unset(); //Callback function
		TimerQueue tq = TimerQueue.instance();
		TimerQueueItem<MutableObject> tqi = null;
		MutableObject obj = null;
		
		for (int i=0; i<50; i++) {
			if (tqi == null || tq.tryDisable(tqi)) {
				obj = new MutableObject(0);
				tqi = new TimerQueueItem<>(obj, listener);
			}
			
			obj.set();
			tq.enableAndAdd(tqi, System.currentTimeMillis()+5);
		}
		
		tq.waitUntilEmpty();
		
		Assertions.assertFalse(obj.isSet());
	}
	
	@Test
	public void bufferedUseTest() {
		Random rand = new Random();
		
		Buffer buffer = new Buffer();
		DeferredWorkReceiver<MutableObject> listener = item -> { //Callback function
			item.unset();
			buffer.release(item);
		};
		
		TimerQueue tq = TimerQueue.instance();
		TimerQueueItem<MutableObject> tqi = null;
		MutableObject obj = null;
		
		for (int i=0; i< 100; i++) {
			if (tqi == null || !tq.tryDisable(tqi)) {
				obj = buffer.acquire();
				tqi = new TimerQueueItem<>(obj, listener);
			}
			
			obj.set();
			tq.enableAndAdd(tqi, System.currentTimeMillis()+3);
			
			try {
				Thread.sleep(rand.nextInt(3)+2); //Between 2 and 4 millis
			} catch (InterruptedException e) { }
		}
		
		tq.waitUntilEmpty();
		
		Assertions.assertTrue(buffer.isZero());
		Assertions.assertFalse(obj.isSet());
	}

	private class MutableObject {
		private int id;
		private boolean state;
		
		public MutableObject(int id) {
			this.id = id;
		}
		
		public void set() {
			state = true;
		}
		
		public void unset() { state = false; }
		
		public boolean isSet() {
			return state;
		}
		
		public int id() { return id; }
	}
	
	private class Buffer { // A buffer with only one item
		private final MutableObject bufObj = new MutableObject(0);
		private int count;
		
		public synchronized MutableObject acquire() {
			count++;
			return bufObj;
		}
		
		public synchronized void release(MutableObject obj) { count--; }
		
		public synchronized boolean isZero() { return count == 0; }
	}
	
	@AfterAll
	static void shutdown() {
		TimerQueue.instance().shutdown();
	}
	
}
