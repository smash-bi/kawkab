package kawkab.fs.core.tq;

import org.junit.jupiter.api.Test;

public class TimerTransferQueueTest {
	public static void main(String args[]) {
		new TimerTransferQueueTest().interfaceTest();
	}

	@Test
	public void interfaceTest() {
		TimerTransferQueue<MutableObject> t = new TimerTransferQueue<MutableObject>();
		for (int i = 0; i < 10; ++i) {
			TimerTransferableWrapper<MutableObject> w = new TimerTransferableWrapper<MutableObject>(new MutableObject(i));
			t.add(w, System.currentTimeMillis() + (i + 1) * 1000);
			// Must disable first before changing.
			if (t.disable(w)) {
				w.getItem().update(i * 10);
				t.enableOrAdd(w, System.currentTimeMillis() + 1000); // Re-enable, add if necessary
				if (i == 7) {
					t.complete(w);
				}
			}
		}
		while (true) {
			try {
				MutableObject item = t.take();
				if (item != null) {
					System.out.println("Printing item: " + item.get());
				} else {
					System.out.println("Encountered disabled object");
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	private class MutableObject {
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
