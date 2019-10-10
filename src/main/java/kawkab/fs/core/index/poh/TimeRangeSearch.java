package kawkab.fs.core.index.poh;

class TimeRangeSearch {
	/**
	 * Searches the given timestamp in the given searches
	 * @param ranges Either entries or pointers
	 * @param length length of the ranges array to search
	 * @param findOldest find the oldest value or the latest value. Oldest has lower segment number.
	 * @param ts Timestamp to search
	 * @return -1 if the timestamp is not found, otherwise returns the index of the first or the last occurrence
	 */
	public static int find(final TimeRange[] ranges, int length, boolean findOldest, final long ts) {
		if (ranges == null || length == 0)
			return -1;

		if (length == 0) //If there is no entry
			return -1;

		// Check if the given TS is out of the range of this node
		if (ranges[0].compare(ts) > 1|| ranges[length-1].compare(ts) < 0) {
			return -1;
		}

		if (findOldest)
			return findFirstIndex(ranges, length, ts);
		else
			return findLastIndex(ranges, length, ts);
	}

	/**
	 * Find the lowest range value that contains the ts
	 *
	 * @param ts Timestamp to search
	 * @return A negative number if the timestamp is not found, otherwise returns the index of the first entry that has ts within its range
	 */
	public static int findFirstIndex(final TimeRange[] ranges, final int length, final long ts) {
		if (ranges == null || length == 0)
			return -1;

		if (ranges[0].compare(ts) > 0) //if the first range is larger than ts
			return -1;

		if (ranges[length-1].compare(ts) < 0) //if the lat range is smaller than ts
			return -1;

		//Check the first entry
		if (ranges[0].compare(ts) == 0) {
			return 0;
		}
		// Do not check the right extreme because we want to find the first matching entry

		int left = 0;
		int right = length - 1;
		while(left < right) {
			int mid = (left + right) >>> 1;

			if (ranges[mid].compare(ts) < 0) { // if the mid entry is less than ts, then all the left sub-array is less than the ts, keep the right part
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
	public static int findLastIndex(final TimeRange[] ranges, final int length, final long ts) {
		if (ranges == null || length == 0)
			return -1;

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

	/**
	 * Find the largest entry that has either ts in its range or the maxTS of the entry is smaller and closest to ts
	 * @param ranges
	 * @param length
	 * @param ts
	 * @return
	 */
	public static int findFloor(final TimeRange[] ranges, final int length, final long ts) {
		if (ranges == null || length == 0)
			return -1;

		int right = length-1;
		TimeRange entry = ranges[right];

		//Check the right most entry
		if (entry.compare(ts) <= 0) {
			return right;
		}

		if (ts < ranges[0].minTS()) {
			return -1;
		}

		int left = 0;
		while(left < right) {
			int mid = left + (right - left + 1)/2; // Take the ceiling value as we want to move the right pointer

			entry = ranges[mid];

			int res = entry.compare(ts);
			//int res = entry.maxTS() <= ts ? 0 : -1;

			if (res <= 0)
				left = mid;
			else
				right = mid - 1;
		}

		return ranges[right].compare(ts) <= 0 ? right : -1;
	}

	/**
	 * Find the smallest entry that has either ts in its range or the minTS of the entry is larger and closest to ts
	 * @param ranges
	 * @param length
	 * @param ts
	 * @return
	 */
	public static int findCeil(final TimeRange[] ranges, final int length, final long ts) {
		if (ranges == null || length == 0)
			return -1;

		TimeRange entry = ranges[0];

		//Check the first entry
		if (entry.compare(ts) >= 0) {
			return 0; // Return the first entry as we want the ceil
		}

		if (ranges[length-1].maxTS() < ts)
			return -1; // ts is larger than the maxTS of the last entry. We don't find any ceil.

		// Do not check the right extreme because we want to find the first matching entry

		int left = 0;
		int right = length - 1;
		while(left < right) {
			int mid = left + (right - left)/2;

			entry = ranges[mid];

			int res = entry.compare(ts);
			//int res = entry.minTS() >= ts ? 0 : 1;

			if (res < 0) { // if entry is less than the ts, then all the left sub-array is less than the ts, keep the right part
				left = mid + 1;
			} else { // if entry is greater than or equal to the ts, then all the right sub-array can be discarded
				right = mid;
			}
		}

		return ranges[left].compare(ts) >= 0 ? left : -1;
	}

	/**
	 * Finds the index of the highest range that is smaller than ts or the lowest ranges that covers ts
	 *
	 * This means, if the lowest range is at index i that covers ts, this function returns r=i-1, keeping r>=0.
	 * For example, if the given timestamps are {1, 3, 3, 4, 4, 5}, searching for 4 will return index 2 that has the value 3.
	 * This is useful because the segment number 3 may have the timestamps of 4.
	 *
	 * @param ranges
	 * @param length
	 * @param ts
	 * @return
	 */
	public static int findFirstFloor(final TimeRange[] ranges, final int length, final long ts) {
		if (ranges == null || length == 0)
			return -1;

		int firstComp = ranges[0].compare(ts);

		if (firstComp > 0) // if ts is smaller than the first entry
			return -1;

		if (firstComp == 0)
			return 0;

		if (ranges[length-1].compare(ts) < 0) // if the last entry is smaller than ts
			return length-1;

		int left = 0;
		int right = length - 1;

		while (left < right) {
			int mid = (left + right + 1) >>> 1;

			if (ranges[mid].compare(ts) >= 0)
				right = mid - 1;
			else
				left = mid;

		}

		return left;
	}

	/**
	 * Find the highest range that covers ts or smaller than ts
	 *
	 * @param ranges
	 * @param length
	 * @param ts
	 * @return
	 */
	public static int findLastFloor(final TimeRange[] ranges, final int length, final long ts) {
		if (ranges == null || length == 0)
			return -1;

		if (ranges[0].compare(ts) > 0) // if ts is smaller than the first entry
			return -1;

		if (ranges[length-1].compare(ts) <= 0) // if the last entry is smaller or equal to ts
			return length-1;

		int left = 0;
		int right = length - 1;

		while (left < right) {
			int mid = (left + right + 1) >>> 1; //Taking the ceil value because we are moving the right pointer when the values are different

			if (ranges[mid].compare(ts) <= 0)
				left = mid;
			else
				right = mid - 1;

		}

		return ranges[left].compare(ts) <= 0 ? left : -1;
	}
}
