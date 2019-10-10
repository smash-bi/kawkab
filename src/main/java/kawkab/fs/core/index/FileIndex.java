package kawkab.fs.core.index;

import kawkab.fs.core.exceptions.KawkabException;

import java.io.IOException;
import java.util.List;

public interface FileIndex {
	/**
	 * Finds the oldest or latest index entry
	 *
	 * @param key timestamp
	 * @param findHighest Whether to return the latest value (largest file offset) or the oldest entry (smallest file offset)
	 * @return -1 if the record is not found, otherwise returns segment number in the file
	 *
	 * @throws IOException
	 * @throws KawkabException
	 */
	long findHighest(final long key);

	/**
	 * Find all entries between the given timestamps inclusively
	 * @param minTS smallest timestamp
	 * @param maxTS largest timestamp
	 * @return null if no records are found, other returns the list of the lists of timestamps
	 */
	List<long[]> findAll(final long minTS, final long maxTS);

	/**
	 * Appends the entry in the index
	 * @param key timestamp
	 * @param segmentInFile segment number in the file
	 *
	 * @throws IOException
	 * @throws KawkabException
	 */
	void append(long key, long segmentInFile) throws IOException, KawkabException;
}
