package kawkab.fs.core.index;

import kawkab.fs.core.exceptions.KawkabException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.List;

public interface FileIndex {
	/**
	 * Finds the oldest or latest index entry
	 *
	 * @param timestamp
	 * @return -1 if the record is not found, otherwise returns segment number in the file
	 *
	 * @throws IOException
	 * @throws KawkabException
	 */
	long findHighest(final long timestamp);

	/**
	 * Find all entries between the given timestamps inclusively based on the minTS of the index entries.
	 * The lowest segment may or may not have the minTS.
	 *
	 * If the index has minTS [1, 2, 3, 3, 3, 5}, searching for the range [3, 4] will return the
	 * four segment numbers that has the timetamps 2 and 3 in the index. This is because this function does not
	 * consider the maxTS to exclude the segment that has minTS 2. Therefore, the segment with minTS 2 may have the
	 * timestamp 3 in one of its records.
	 *
	 * @param minTS smallest timestamp
	 * @param maxTS largest timestamp
	 * @return null if no records are found, other returns the list of the lists of timestamps
	 */
	List<long[]> findAllMinBased(final long minTS, final long maxTS);

	/**
	 * This function returns the segment numbers of all the segments that have minTS and maxTS inclusively.
	 *
	 * @param minTS
	 * @param maxTS
	 * @return
	 */
	List<long[]> findAll(final long minTS, final long maxTS);

	/**
	 * Appends the timestamp of the first record in the segment number segmentInFile
	 *
	 * The function call to this function must precede the call to appendMaxTS.
	 *
	 * @param minTS timestamp
	 * @param segmentInFile segment number in the file
	 *
	 * @throws IOException
	 * @throws KawkabException
	 */
	void appendMinTS(final long minTS, final long segmentInFile);

	/**
	 * Appends the timestamp of the last record in the segment number segmentInFile. This function must be called
	 * after the corresponding appendMinTS
	 *
	 * semgentInFile must match with the last index entry's segmentInFile that was provided through appendMinTS function call.
	 *
	 * @param maxTS timestamp
	 * @param segmentInFile segment number in the file
	 *
	 * @throws IOException
	 * @throws KawkabException
	 */
	void appendMaxTS(long maxTS, long segmentInFile);

	void appendIndexEntry(final long minTS, final long maxTS, final long segmentInFile) ;

	/**
	 * Load the index from the local or the global store
	 *
	 * The length of the index must already be set.
	 */
	void loadAndInit() throws IOException, KawkabException;

	void shutdown() throws KawkabException;

	int storeTo(final ByteBuffer buffer);

	int loadFrom(final ByteBuffer buffer) throws IOException;
}
