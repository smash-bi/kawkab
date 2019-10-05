package kawkab.fs.core.index.poh;

import kawkab.fs.core.index.poh.TimeRange;

public class TimeRangeSearch {
	/**
	 * Searches the given timestamp in the given searches
	 * @param ranges Either entries or pointers
	 * @param length length of the ranges array to search
	 * @param firstOccurrence return the first or the last occurrence, true if the first occurrence
	 * @param ts Timestamp to search
	 * @return -1 if the timestamp is not found, otherwise returns the index of the first or the last occurrence
	 */
	public static int find(final TimeRange[] ranges, int length, boolean firstOccurrence, final long ts) {
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
	public static int findFirstIndex(final TimeRange[] ranges, final int length, final long ts) {
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
	public static int findLastIndex(final TimeRange[] ranges, final int length, final long ts) {
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
}
