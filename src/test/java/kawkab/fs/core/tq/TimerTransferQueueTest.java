package kawkab.fs.core.tq;

import kawkab.fs.core.timerqueue.DeferredWorkReceiver;
import org.junit.jupiter.api.Test;

public class TimerTransferQueueTest implements DeferredWorkReceiver<TimerTransferQueueTest.MutableObject> {
	public static void main(String args[]) {
		new TimerTransferQueueTest().interfaceTest();
	}

	@Test
	public void interfaceTest() {
		TimerTransferQueue<MutableObject> t = new TimerTransferQueue<MutableObject>("Test Queue");
		for (int i = 0; i < 10; ++i) {
			TimerTransferableWrapper<MutableObject> w = new TimerTransferableWrapper<MutableObject>(new MutableObject(i), this);
			t.add(w, System.currentTimeMillis() + (i + 1) * 1000);
			// Must disable first before changing.
			if (t.disable(w)) {
				w.getItem().update(i * 10);
				System.out.println("enable and add item " + (i));
				t.enableOrAdd(w, System.currentTimeMillis() + (i+1) * 1200); // Re-enable, add if necessary
				/*if (i == 7) {
					t.complete(w);
				}*/
			}

		}
		/*while (true) {
			try {
				//MutableObject item = t.take();
				TimerTransferableWrapper<MutableObject> w = t.take();
				if (w != null) {
					MutableObject item = w.currentItem();
					System.out.println("Printing item: " + item.get());
				} else {
					System.out.println("Encountered disabled object");
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}*/
		try {
			Thread.sleep(30000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void deferredWork(MutableObject item) {
		System.out.println("Printing item: " + item.get());
	}

	public class MutableObject {
		private int i;
		public MutableObject(int i) {
			this.i = i;
		}

		public void update(int i) {
			this.i = i;
		}

		public int get() {
			return i;
		}
	}
}
