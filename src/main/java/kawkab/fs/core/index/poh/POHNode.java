package kawkab.fs.core.index.poh;

import kawkab.fs.core.exceptions.IndexBlockFullException;

/**
 * The class is not thread safe for concurrent appends. However, the search and append can be concurrent.
 */
class POHNode {
	private final int number; //node number
	private final POHEntry[] entries; // Number of index entries
	private final POHPointer[] pointers;

	private int entryIdx;
	private int pointerIdx;

	/**
	 * @param nodeNum The node number in the k-ary post-order heap
	 * @param entriesCount Number of index entries in the node, must be greater than zero so that we have at least one index entry
	 * @param pointersCount Number of pointers or children of each node, must be greater than one so that we have at least a binary tree
	 */
	POHNode(final int nodeNum, final int entriesCount, final int pointersCount) {
		assert entriesCount > 0;
		assert pointersCount > 1;

		number = nodeNum;
		entries = new POHEntry[entriesCount];
		pointers = new POHPointer[pointersCount];
	}

	/**
	 * Appends an entry in the node
	 *
	 * minTS is required to be greater or equal to the last maxTS
	 *
	 * segInFile must be greater or equal to the last segInFile
	 *
	 * minTS must be smaller or equal to maxTS
	 *
	 * The caller must ensure that there are no concurrent appendEntry calls to the same POHNode
	 *
	 * @param minTS minimum timestamp in the segment
	 * @param maxTS maximum timestamp in the segment
	 * @param segInFile segment number in the file
	 * @throws IllegalArgumentException if the preconditions are not met
	 * @throws IndexBlockFullException if the block is already full with the index entries
	 */
	void appendEntry(final long minTS, final long maxTS, final long segInFile)
			throws IllegalArgumentException, IndexBlockFullException {

		assert pointerIdx == 0 || pointerIdx == pointers.length :
				"Either the pointers should not exist or they all be appended before the entries; pointerIdx="
						+pointerIdx+", limit="+pointers.length;

		if (entryIdx == entries.length) {
			throw new IndexBlockFullException("No space left in the POHNode to add more index entries");
		}

		POHEntry last = entries[entryIdx];

		if (last != null) {
			if (minTS < last.maxTS())
				throw new IllegalArgumentException("Current minTS must be greater or equal to last maxTS; minTS="
						+ minTS + ", last maxTS=" + last.maxTS());

			if (segInFile < last.segInFile()) {
				throw new IllegalArgumentException("Current segInFile must be equal or larger than the last segInFile; segInFile="
						+ segInFile + ", last segInFile=" + last.segInFile());
			}

			if (minTS > maxTS) {
				throw new IllegalArgumentException("minTS should be <= maxTS; minTS=" + minTS + ", maxTS=" + maxTS);
			}
		}

		entries[entryIdx++] = new POHEntry(minTS, maxTS, segInFile);
	}

	/**
	 * nodeNum must be smaller than this node's number
	 *
	 * minTS must be smaller or equal to maxTS
	 *
	 * maxTS of the last pointer must be equal or smaller than minTS
	 *
	 * @param minTS minTS in node number nodeNum
	 * @param maxTS maxTS in node number nodeNum
	 * @param nodeNum Node number in the poster-order heap
	 * @throws IndexBlockFullException if the node is already full with the pointers
	 */
	void appendPointer(final long minTS, final long maxTS, final int nodeNum) throws IndexBlockFullException {
		assert entryIdx == 0; //The pointers must be added before the entries to make this node append-only

		if (pointerIdx == pointers.length)
			throw new IndexBlockFullException();

		POHPointer last = pointers[pointerIdx];

		if (last != null) {
			if (nodeNum >= number)
				throw new IllegalArgumentException("nodeNum of the pointer must be smaller than the current node, nodeNum="
						+nodeNum+", current nodeNum="+number);

			if (minTS > maxTS)
				throw new IllegalArgumentException("minTS should be <= maxTS; minTS=" + minTS + ", maxTS=" + maxTS);

			if (minTS >= last.maxTS())
				throw new IllegalArgumentException("Current minTS must be greater or equal to the last pointer's maxTS; minTS="
						+ minTS + ", last maxTS=" + last.maxTS());
		}

		pointers[pointerIdx++] = new POHPointer(nodeNum, minTS, maxTS);
	}

	/**
	 * Returns the node numbers of all the children nodes that contain the given timestamp
	 * @param ts ts to search
	 * @return null if no entry found, otherwise returns the array that contains the node numbers that have the timestamp
	 */
	int[] findAllPointers(final long ts) {
		int leftIdx = find(pointers, pointerIdx, true, ts);

		if (leftIdx == -1) // no entry found
			return null;

		int rightIdx = find(pointers, pointerIdx, false, ts);

		int[] nodeNums = new int[rightIdx - leftIdx + 1];
		for (int i=0; i<nodeNums.length; i++) {
			nodeNums[i] = pointers[i+leftIdx].nodeNum();
		}

		return nodeNums;
	}

	/**
	 * Returns the node number of the left most child node that contain the given timestamp
	 *
	 * @param ts ts to search
	 * @return -1 if no entry found, otherwise returns the node number of the left most child that has the timestamp
	 */
	int findFirstPointer(final long ts) {
		int idx = find(pointers, pointerIdx, true, ts);
		if (idx < 0)
			return -1;

		return pointers[idx].nodeNum();
	}

	/**
	 * Returns the node number of the right most child that contain the given timestamp
	 *
	 * @param ts ts to search
	 * @return -1 if no entry found, otherwise returns the node number of the right most child that has the timestamp
	 */
	int findLastPointer(final long ts) {
		int idx = find(pointers, pointerIdx, false, ts);
		if (idx < 0)
			return -1;

		return pointers[idx].nodeNum();
	}

	/**
	 * Returns the segment number of all the segments that contain the given timestamp
	 * @param ts ts to search
	 * @return null if no entry found, otherwise returns the array that contains the segment numbers that have the timestamp
	 */
	long[] findAllEntries(final long ts) {
		int leftIdx = find(entries, entryIdx, true, ts);

		if (leftIdx == -1) // no entry found
			return null;

		int rightIdx = find(entries, entryIdx, false, ts);

		long[] segs = new long[rightIdx - leftIdx + 1];
		for (int i=0; i<segs.length; i++) {
			segs[i] = entries[i+leftIdx].segInFile();
		}

		return segs;
	}

	/**
	 * Returns the segment number of the first segment that contain the given timestamp
	 *
	 * @param ts ts to search
	 * @return -1 if no entry found, otherwise returns the first occurrence of the segment that has the timestamp
	 */
	long findFirstEntry(final long ts) {
		int idx = find(entries, entryIdx, true, ts);
		if (idx < 0)
			return -1;

		return entries[idx].segInFile();
	}

	/**
	 * Returns the segment number of the last segment that contain the given timestamp
	 *
	 * @param ts ts to search
	 * @return -1 if no entry found, otherwise returns the last occurrence of the segment that has the timestamp
	 */
	long findLastEntry(final long ts) {
		int idx = find(entries, entryIdx, false, ts);
		if (idx < 0)
			return -1;

		return entries[idx].segInFile();
	}

	/**
	 * Searches the given timestamp in the given searches
	 * @param ranges Either entries or pointers
	 * @param length length of the ranges array to search
	 * @param firstOccurrence return the first or the last occurrence, true if the first occurrence
	 * @param ts Timestamp to search
	 * @return -1 if the timestamp is not found, otherwise returns the index of the first or the last occurrence
	 */
	private int find(final TimeRange[] ranges, int length, boolean firstOccurrence, final long ts) {
		if (length == 0) //If there is no entry
			return -1;

		// Check if the given TS is out of the range of this node
		if (ranges[0].minTS() > ts || ranges[length-1].maxTS() < ts) {
			return -1;
		}

		if (firstOccurrence)
			return findFirstIndex(ranges, length, ts);
		else
			return findLastIndex(ranges, length, ts);
	}

	/**
	 * @param ts Timestamp to search
	 * @return A negative number if the timestamp is not found, otherwise returns the index of the first entry that has ts within its range
	 */
	private int findFirstIndex(final TimeRange[] ranges, final int length, final long ts) {
		TimeRange entry = ranges[0];

		//Check the first entry
		if (entry.compare(ts) == 0) {
			return 0;
		}
		// Do not check the right extreme because we want to find the first matching entry

		int left = 0;
		int right = length - 1;
		while(left < right) {
			int mid = left + (right - left)/2;

			entry = ranges[mid];

			int res = entry.compare(ts);

			if (res < 0) { // if entry is less than the ts, then all the left sub-array is less than the ts, keep the right part
				left = mid + 1;
			} else { // if entry is greater than or equal to the ts, then all the right sub-array can be discarded
				right = mid;
			}
		}

		return ranges[left].compare(ts) == 0 ? left : -1;
	}

	/**
	 * @param ts Timestamp to search
	 * @return A negative number if the timestamp is not found, otherwise returns the index of the last entry that has ts within its range
	 */
	private int findLastIndex(final TimeRange[] ranges, final int length, final long ts) {
		int right = length-1;
		TimeRange entry = ranges[right];

		//Check the right most entry
		if (entry.compare(ts) == 0) {
			return right;
		}
		// Do not check the left extreme because we want to find the first matching entry

		int left = 0;
		while(left < right) {
			int mid = left + (right - left + 1)/2; // Take the ceiling value as we want to move the right pointer

			entry = ranges[mid];

			int res = entry.compare(ts);

			if (res <= 0)
				left = mid;
			else
				right = mid - 1;
		}

		return ranges[right].compare(ts) == 0 ? right : -1;
	}
}
