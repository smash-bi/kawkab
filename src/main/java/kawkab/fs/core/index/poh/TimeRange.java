package kawkab.fs.core.index.poh;

class TimeRange {
	final long minTS;
	final long maxTS;

	TimeRange(long minTS, long maxTS) {
		this.minTS = minTS;
		this.maxTS = maxTS;
	}

	long minTS() {
		return minTS;
	}

	long maxTS() {
		return maxTS;
	}

	/**
	 * Compare the given timestamp with the entry
	 * @param ts
	 * @return 0 if ts is within the range of this entry, -1 if this entry is lower than ts, 1 if this entry is greater than ts
	 */
	int compare(long ts) {
		if (minTS <= ts && ts <= maxTS)
			return 0;

		if (maxTS < ts)
			return -1;

		return 1;
	}
}
