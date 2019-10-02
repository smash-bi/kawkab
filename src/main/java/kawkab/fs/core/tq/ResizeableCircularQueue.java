package kawkab.fs.core.tq;

class ResizeableCircularQueue <T>{
	private T[] q;
	// Note that end is one past the end of the array
	private int start, end, size, capacity;
	
	
	@SuppressWarnings("unchecked")
	public ResizeableCircularQueue(int initCapacity) {
		capacity = initCapacity;
		q = (T[])new Object[capacity];
	}
	
	public void add(T item) {
		// Never shrink the size of the array
		if (size >= capacity) {
			copyCollectAndDouble();
			assert (size < capacity);
		}
		// Update end and size
		q[end++] = item;
		if (end >= capacity) {
			end = 0;
		}
		size++;
	}
	
	public int frontIndex() {
		// assert(size > 0) : "size is "+size;
		return size > 0 ? start : -1;
	}
	
	// Should only be called if it is not empty
	public void pop() {
		assert(size > 0);
		q[start++] = null; // To help with garbage collection
		if (start >= capacity) {
			start = 0;
		}
		size--;
	}
	
	public T getAtIndex(int index) {
		return q[index];
	}
	
	private void copyCollectAndDouble() {
		int newCapacity = capacity * 2;
		@SuppressWarnings("unchecked")
		T[] newQ = (T[])new Object[newCapacity];
		int oldIterator = start;
		for (int i = 0; i < size; ++i) {
			newQ[i] = q[oldIterator++];
			if (oldIterator >= capacity) {
				oldIterator = 0;
			}
		}
		// Overwrite the old queue
		q = newQ;
		capacity = newCapacity;
		start = 0;
		end = size;
	}
	
	public int size() {
		return size;
	}
}
