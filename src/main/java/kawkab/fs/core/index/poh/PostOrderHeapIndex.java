package kawkab.fs.core.index.poh;

import java.util.ArrayList;

/**
 * Nodes in a post-order heap are created in the post-order, i.e., the parent node is created after the children. It is
 * an implicit data structure.
 *
 * The class supports only one modifier thread. The concurrent modifiers should synchronized externally.
 */

public class PostOrderHeapIndex {
	private ArrayList<POHNode> nodes;	//This is an append-only list. The readers should read but not modify the list. The sole writer can a[[emd new nodes.

	// Configuration parameters
	private final int fanout;			//Branching factor of the tree
	private final int entriesPerNode;	//Number of index entries in each node

	/**
	 *
	 * @param fanout
	 * @param entriesPerNode
	 */
	public PostOrderHeapIndex(final int fanout, final int entriesPerNode) {
		nodes = new ArrayList<>();

		this.fanout = fanout;
		this.entriesPerNode = entriesPerNode;
	}

	/**
	 * Appends the timestamps to the index
	 *
	 * This function must be called by a single thread.
	 *
	 * @param minTS minimum timestamp in the data segment
	 * @param maxTS maximum timestamp in the data segment
	 */
	public void insert(long minTS, long maxTS) {

	}

	/**
	 * Searches the timestamp in the index.
	 *
	 * @param ts timestamp to find
	 * @return the segment in file that contains the timestamp ts
	 */
	public long findFirst(long ts) {
		//FIXME: What if we have same entries across nodes?

		return -1;
	}

	public long findLast(long ts) {
		return -1;
	}

	public long[] findAll(long ts) {
		return null;
	}
}
