package kawkab.fs.core.index.poh;

public interface TimeRange {
	/**
	 * Compare the given timestamp with the entry
	 *
	 * Follows the Java Comparator contract
	 *
	 * @param ts
	 * @return 0 if ts is within the range of this entry, -1 if this entry is lower than ts, 1 if this entry is greater than ts
	 */
	int compare(long ts);

	long minTS();

	long maxTS();
}
