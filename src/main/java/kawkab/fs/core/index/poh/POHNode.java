package kawkab.fs.core.index.poh;

import kawkab.fs.core.exceptions.IndexBlockFullException;

/**
 * The class is not thread safe for concurrent appends. However, the search and append can be concurrent.
 */
class POHNode implements TimeRange {
	private final int nodeNumber; //node number
	private final int height;
	private final POHEntry[] entries; // Number of index entries
	private final POHNode[] children;

	private int entryIdx;
	private int pointerIdx;

	private long entryMinTS = Long.MAX_VALUE;
	private long entryMaxTS;
	private long childMinTS = Long.MAX_VALUE;
	private long childMaxTS;

	/**
	 * @param nodeNum The node number in the k-ary post-order heap
	 * @param entriesCount Number of index entries in the node, must be greater than zero so that we have at least one index entry
	 * @param pointersCount Number of pointers or children of each node, must be greater than one so that we have at least a binary tree
	 */
	POHNode(final int nodeNum, final int height, final int entriesCount, final int pointersCount) {
		assert entriesCount > 0;
		assert pointersCount > 1;

		nodeNumber = nodeNum;
		this.height = height;
		entries = new POHEntry[entriesCount];
		children = new POHNode[pointersCount];
	}

	static int pointerSizeBytes() {
		return Long.BYTES + Integer.BYTES; //Two timestamps and a node number
	}

	/**
	 * Appends an entry in the node
	 *
	 * minTS is required to be greater or equal to the last maxTS
	 *
	 * offsetInFile must be greater than the last offsetInFile
	 *
	 * minTS must be smaller or equal to maxTS
	 *
	 * The caller must ensure that there are no concurrent appendEntry calls to the same POHNode
	 *
	 * @param timestamp timestamp in the record
	 * @param offsetInFile Byte offset in file
	 * @throws IllegalArgumentException if the preconditions are not met
	 * @throws IndexBlockFullException if the block is already full with the index entries
	 */
	void appendEntry(final long timestamp, final long offsetInFile)
			throws IllegalArgumentException, IndexBlockFullException {

		assert pointerIdx == 0 || pointerIdx == children.length :
				"Either the pointers should not exist or they all be appended before the entries; pointerIdx="
						+pointerIdx+", limit="+ children.length;

		if (entryIdx == entries.length) {
			throw new IndexBlockFullException("No space left in the POHNode to add more index entries");
		}

		POHEntry last = null;

		if (entryIdx > 0)
			last = entries[entryIdx-1];

		// Check the arguments
		if (last != null) {
			if (timestamp < last.timestamp())
				throw new IllegalArgumentException("Current minTS must be greater or equal to last entry's timestamp; timestamp="
						+ timestamp + ", last maxTS=" + last.timestamp());

			if (offsetInFile <= last.segmentInFile()) {
				throw new IllegalArgumentException("Current offsetInFile must be greater than the last offsetInFile; offsetInFile="
						+ offsetInFile + ", last offsetInFile=" + last.segmentInFile());
			}
		}

		// Append the entry
		entries[entryIdx++] = new POHEntry(timestamp, offsetInFile);

		// Update the minTS and maxTS for this node. minTS will not change if this node is an internal node.
		if (this.childMinTS > timestamp) // This will change only for leaf nodes
			this.childMinTS = timestamp;

		if (entryMinTS > timestamp)
			entryMinTS = timestamp;

		if (this.entryMaxTS < timestamp)
			this.entryMaxTS = timestamp;

		if (this.childMaxTS < timestamp)
			this.childMaxTS = timestamp;
	}

	/**
	 * nodeNum must be smaller than this node's number
	 *
	 * minTS must be smaller or equal to maxTS
	 *
	 * maxTS of the last pointer must be equal or smaller than minTS
	 *
	 * @param child Child node
	 * @throws IndexBlockFullException if the node is already full with the pointers
	 */
	void appendChild(final POHNode child) throws IndexBlockFullException {
		assert entryIdx == 0; //The pointers must be added before the entries to make this node append-only

		if (pointerIdx == children.length)
			throw new IndexBlockFullException();

		POHNode last = children[pointerIdx];

		if (child.nodeNumber() >= nodeNumber)
			throw new IllegalArgumentException("nodeNum of the child must be smaller than the current node, child nodeNum="
					+child.nodeNumber()+", current nodeNum="+ nodeNumber);

		if (last != null) {
			if (last.nodeNumber() >= child.nodeNumber())
				throw new IllegalArgumentException("nodeNum of the child must be larger than the last child, child nodeNum="
						+child.nodeNumber()+", last nodeNum="+ last.nodeNumber());

			if (child.childMinTS >= last.entryMaxTS)
				throw new IllegalArgumentException("Current minTS must be greater or equal to the last pointer's maxTS; minTS="
						+ child.childMinTS + ", last maxTS=" + last.entryMaxTS);
		}

		children[pointerIdx++] = child;

		// As this function is called, this is an internal node. So we should update the minimum timestamp of this node from the children.
		if (this.childMinTS > child.childMinTS)
			this.childMinTS = child.childMinTS;

		if (this.childMaxTS < child.childMaxTS) {
			this.childMaxTS = child.childMaxTS;
			this.entryMaxTS = this.childMaxTS; // This will be updated when the first index entry is added
		}
	}

	/**
	 * Returns the node number of the left most child node that contain the given timestamp
	 *
	 * @param ts ts to search
	 * @return -1 if no entry found, otherwise returns the node number of the left most child that has the timestamp
	 */
	int findFirstChild(final long ts) {
		int idx = TimeRangeSearch.find(children, pointerIdx, true, ts);
		if (idx < 0)
			return -1;

		return children[idx].nodeNumber();
	}

	/**
	 * Returns the node number of the right most child that contain the given timestamp
	 *
	 * @param ts ts to search
	 * @return -1 if no entry found, otherwise returns the node number of the right most child that has the timestamp
	 */
	int findLastChild(final long ts) {
		int idx = TimeRangeSearch.find(children, pointerIdx, false, ts);
		if (idx < 0)
			return -1;

		return children[idx].nodeNumber();
	}

	/**
	 * Returns the segments number of all the index entries between minTS and maxTS inclusively
	 *
	 * minTS must be less than or equal to maxTS
	 *
	 * @param minTS
	 * @param maxTS
	 * @return null if no entries are found, otherwise returns the list in the descending order
	 */
	long[] findAllEntries(final long minTS, final long maxTS) {
		assert minTS <= maxTS;

		if (entries.length == 0)
			return null;

		if (entries[0] == null)
			return null;

		if (maxTS < entries[0].timestamp()) // if the first entry is larger than the given range
			return null;

		int length = entryIdx;

		// Find the highest range that is equal or smaller than maxTS
		int rightIdx = length - 1; // Assume that the last entry is smaller or equal than maxTS
		if (maxTS < entries[rightIdx].timestamp())
			rightIdx = TimeRangeSearch.findLastFloor(entries, length, maxTS);

		// Find the highest range that is smaller than minTS or the lowest range that equals ts
		int leftIdx = 0; // Assume that the first entry is larger or equal to minTS
		if (entries[0].timestamp() < minTS) { // if the first entry is smaller than minTS
			leftIdx = TimeRangeSearch.findFirstFloor(entries, rightIdx-leftIdx+1, minTS);
		}

		if (leftIdx == -1 && rightIdx == -1 || leftIdx > rightIdx) // no entry found
			return null;

		if (leftIdx == -1)
			//leftIdx = 0;
			assert false;
		else if (rightIdx == -1)
			//rightIdx = length - 1;
			assert false;

		long[] segs = new long[rightIdx - leftIdx + 1];
		for (int i=0; i<segs.length; i++) {
			// segs[i] = entries[i+leftIdx].offsetInFile(); //Ascending order
			segs[i] = entries[rightIdx-i].segmentInFile(); //Descending order
		}

		return segs;
	}

	/**
	 * Returns the segment numbers of all the segments that may contain the given timestamp
	 * @param ts ts to search
	 * @return null if no entry found, otherwise returns the array that contains the segment numbers that have the timestamp
	 */
	long[] findAllEntries(final long ts) {
		if (entries.length == 0)
			return null;

		return findAllEntries(ts, ts);
		/*int length = entryIdx;

		int leftIdx = TimeRangeSearch.find(entries, length, true, ts);

		if (leftIdx == -1) // no entry found
			return null;

		int rightIdx = TimeRangeSearch.find(entries, length, false, ts);

		long[] segs = new long[rightIdx - leftIdx + 1];
		for (int i=0; i<segs.length; i++) {
			// segs[i] = entries[i+leftIdx].offsetInFile(); //Ascending order
			segs[i] = entries[rightIdx-i].offsetInFile(); //Descending order
		}

		return segs;*/
	}

	/**
	 * Returns the highest index entry that has the range smaller than ts or the smallest index entry that
	 * covers ts
	 *
	 * @param ts ts to search
	 * @return -1 if no entry found, otherwise returns the first occurrence of the segment that has the timestamp
	 */
	long findFirstEntry(final long ts) {
		if (entries.length == 0)
			return -1;

		int idx = TimeRangeSearch.findFirstFloor(entries, entryIdx, ts);
		if (idx < 0)
			return -1;

		return entries[idx].segmentInFile();
	}

	/**
	 * Returns the highest index entry that has the range smaller than ts or the highest index entry that
	 * covers ts
	 *
	 * @param ts ts to search
	 * @return -1 if no entry found, otherwise returns the last occurrence of the segment that has the timestamp
	 */
	long findLastEntry(final long ts) {
		if (entries.length == 0)
			return -1;

		int idx = TimeRangeSearch.findLastFloor(entries, entryIdx, ts);
		if (idx < 0)
			return -1;

		return entries[idx].segmentInFile();
	}

	int nodeNumber () {
		return nodeNumber;
	}

	boolean isFull () {
		return entryIdx == entries.length;
	}

	/**
	 * Returns the node number of the nthChild of the current node
	 *
	 * @param nthChild Child number, starting from 1
	 * @return -1 if child does not exist, otherwise returns the node number of the nthChild
	 */
	private int childNodeNumber(int nthChild) {
		assert nthChild > 0;
		assert nthChild <= children.length;

		POHNode child = children[nthChild-1];
		if (child == null)
			return -1;

		return child.nodeNumber();
	}

	POHNode nthChild(int childNumber) {
		assert childNumber > 0;
		assert childNumber <= children.length;

		return children[childNumber-1];
	}

	long entryMinTS() {
		return entryMinTS;
	}

	int height () {
		return height;
	}

	@Override
	public long minTS() {
		return childMinTS;
	}

	@Override
	public long maxTS() {
		return entryMaxTS;
	}

	@Override
	public int compare(long ts) {
		if (childMinTS <= ts && ts <= entryMaxTS)
			return 0;

		if (entryMaxTS < ts)
			return -1;

		return 1;
	}
}
