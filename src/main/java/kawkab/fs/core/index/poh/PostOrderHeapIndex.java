package kawkab.fs.core.index.poh;

import kawkab.fs.core.exceptions.IndexBlockFullException;
import kawkab.fs.core.index.FileIndex;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Nodes in a post-order heap are created in the post-order, i.e., the parent node is created after the children. It is
 * an implicit data structure.
 *
 * The class supports only one modifier thread. The concurrent modifiers should synchronized externally.
 */

public class PostOrderHeapIndex implements FileIndex {
	private final double logBase;
	private ArrayList<POHNode> nodes;	//This is an append-only list. The readers should read but not modify the list. The sole writer can a[[emd new nodes.

	// Configuration parameters
	private final int childrenPerNode; //Branching factor of the tree
	private final int entriesPerNode; //Number of index entries in each node
	private final int nodeSizeBytes;
	private POHNode currentNode;

	// Number of timestamps in the index. The length divided by 2 shows the number of index entries in the index. Moreover, if the length is an odd number, the last index entry is half complete.
	private AtomicLong length = new AtomicLong(0);

	//TODO: This table should final and static
	private int[] nodesCountTable; // Number of nodes in the perfect k-ary tree of height i, i>=0. We use this table to avoid the complex math functions

	/**
	 *
	 * @param indexNodeSizeBytes
	 * @param percentEntriesPerNode
	 */
	public PostOrderHeapIndex(int indexNodeSizeBytes, int percentEntriesPerNode) {
		int entrySize = POHEntry.sizeBytes();
		int pointerSize = POHNode.pointerSizeBytes();

		entriesPerNode = (int) (indexNodeSizeBytes/100.0*percentEntriesPerNode / entrySize);
		childrenPerNode = (int) ((indexNodeSizeBytes-(entriesPerNode*entrySize))*1.0 / pointerSize);

		nodeSizeBytes = indexNodeSizeBytes;

		logBase = Math.log(childrenPerNode);

		init();
	}

	// FIXME: Duplicate code in the constructors
	public PostOrderHeapIndex(int entriesPerNode, int childrenPerNode, int nodeSizeBytes) {
		this.entriesPerNode = entriesPerNode;
		this.childrenPerNode = childrenPerNode;
		this.nodeSizeBytes = nodeSizeBytes;

		logBase = Math.log(childrenPerNode);

		init();

	}

	private void init() {
		//System.out.printf("Index entries per node:  %d\n", entriesPerNode);
		//System.out.printf("Index pointers per node: %d\n", childrenPerNode);

		nodes = new ArrayList<>();
		currentNode = new POHNode(1, 0, entriesPerNode, childrenPerNode);
		nodes.add(null); // Add a dummy value to match the node number with the array index. We do this to simplify the calculation of the index of the children of a node
		nodes.add(currentNode);

		//TODO: Use a systematic way to get a good table size
		nodesCountTable = new int[51]; // We don't expect the height to grow large because of the large branching factor
		for (int i=0; i<nodesCountTable.length; i++) {
			nodesCountTable[i] = totalNodesKAryTree(i);
		}
	}

	private POHNode createNewNode(final int nodeNumber) {
		int height = heightOfNode(nodeNumber);
		POHNode node = new POHNode(nodeNumber, height, entriesPerNode, childrenPerNode);
		if (height > 0) {
			for (int i = 1; i <= childrenPerNode; i++) {
				POHNode child = nodes.get(nthChild(nodeNumber, height, i));
				try {
					node.appendChild(child);
				} catch (IndexBlockFullException e) {
					e.printStackTrace();
					return null;
				}
			}
		}
		return node;
	}

	/**
	 * Appends the segment number and the timestamp of the first entry in the segment
	 *
	 * This function is not thread safe.
	 *
	 * @param minTS timestamp in the data segment
	 * @param segmentInFile Segment number in the file that has the record with the timestamp
	 */
	@Override
	public void appendMinTS(final long minTS, final long segmentInFile) {
		//TODO: check the arguments

		// Current node should not be null.
		// If current node is full,
		// 		create a new node,
		// 		set the new node to the current node
		// 		update the pointers
		//		insert the new index value
		// Add the new node in the nodes array

		long curLength = length.get();

		if (currentNode.isFull()) {
			assert (curLength % 2) == 0;

			currentNode = createNewNode(currentNode.nodeNumber()+1);
			if (currentNode == null)
				return;

			nodes.add(currentNode);
		}

		try {
			currentNode.appendEntryMinTS(minTS, segmentInFile);
		} catch (IndexBlockFullException e) {
			e.printStackTrace();
		}

		length.incrementAndGet();

		//System.out.println("Min: " + minTS + ", seg: " + segmentInFile);
	}

	@Override
	public void appendMaxTS(final long maxTS, final long segmentInFile) {
		long curLength = length.get();

		assert curLength % 2 == 1;
		assert currentNode != null;

		currentNode.appendEntryMaxTS(maxTS, segmentInFile);

		length.incrementAndGet();

		//System.out.println("\tMax: " + maxTS + ", seg: " + segmentInFile);
	}

	@Override
	public void appendIndexEntry(final long minTS, final long maxTS, final long segmentInFile) {
		assert minTS <= maxTS;

		long curLength = length.get();

		if (currentNode.isFull()) {
			assert (curLength % 2) == 0;

			currentNode = createNewNode(currentNode.nodeNumber()+1);
			if (currentNode == null)
				return;

			nodes.add(currentNode);
		}

		try {
			currentNode.appendEntry(minTS, maxTS, segmentInFile);
		} catch (IndexBlockFullException e) {
			e.printStackTrace();
		}

		length.addAndGet(2);
	}

	/**
	 * Searches the timestamp in the index.
	 *
	 * @param ts timestamp to find
	 *
	 * @return the segment number in file that contains ts, or -1 if no entry found
	 */
	@Override
	public long findHighest(long ts) {
		long curLen = length.get();
		int lastNodeIdx = lastNodeIndex(curLen, entriesPerNode);
		int curNode = findNode(ts, true, lastNodeIdx);

		if (curNode == -1)
			return -1;

		return nodes.get(curNode).findLastEntry(ts);
	}

	/**
	 * Find all the data segments that can have the timestamps minTS and maxTS. The segment numbers returned have the first record's
	 * timestamp smaller than or equal to minTS. This indicates that the lowest segment number may or may not have the minTS record.
	 *
	 * The returned list is in descending order of the segment numbers
	 *
	 * @param minTS
	 * @return null if no such segment is found
	 */
	@Override
	public List<long[]> findAllMinBased(final long minTS, final long maxTS) {
		// Find the right most node that has maxTS
		// Traverse from that node to the left until the first value smaller than minTS is reached

		assert minTS <= maxTS;

		long curLen = length.get();
		int lastNodeIdx = lastNodeIndex(curLen, entriesPerNode);
		int curNode = findNode(maxTS, true, lastNodeIdx);

		if (curNode == -1)
			return null;

		List<long[]> results = new ArrayList<>();

		while(curNode > 0) {
			POHNode node = nodes.get(curNode);

			long[] res = node.findAllEntriesMinBased(minTS, maxTS);

			assert res != null;

			results.add(res);

			if (node.entryMinTS() < minTS)
				break;

			curNode--;
		}

		return results;
	}

	@Override
	public List<long[]> findAll(final long minTS, final long maxTS) {
		// Moving to the roots of the trees on the left, find the right-most root node N that has maxTS between minChildTS and maxEntryTS
		// Traverse the tree rooted at N until the node K that has the maxTS between minEntryTS and maxEntryTS
		// While traversing down the tree, select the right most child that has minChildTS less than or equal to maxTS
		// Starting from the node K, add K.findAll(minTS, maxTS), and keep moving to the lower node numbers until
		// the node has maxEntryTS less than minTS.

		assert minTS <= maxTS;

		long curLen = length.get();
		int lastNodeIdx = lastNodeIndex(curLen, entriesPerNode);
		int curNode = findNode(maxTS, true, lastNodeIdx);

		if (curNode <= 0) {
			return checkLastNode(nodes.get(lastNodeIdx), minTS, curLen);
		}

		List<long[]> results = new ArrayList<>();
		if (curNode == lastNodeIdx) {
			long[] res = checkLastEntry(nodes.get(lastNodeIdx), minTS, curLen);
			if (res != null)
				results.add(res);
		}

		while(curNode > 0) {
			POHNode node = nodes.get(curNode);

			long[] res = node.findAllEntries(minTS, maxTS);

			if (res == null) { //This can happen if the given range is larger than the last index entry
				break;
			}

			results.add(res);

			if (node.entryMinTS() < minTS)
				break;

			curNode--;
		}


		if (results.size() == 0)
			return null;

		return results;
	}

	/**
	 * If the last index entry is half open, i.e., it has minTS but the maxTS is not updated yet, we need to check
	 * the last segment. If the minTS is greater than the last entry's maxTS, we should add the last segment in the
	 * results.
	 *
	 * @return
	 */
	private void checkLastNode(final POHNode lastNode, final long minTS, final long curLen, final List<long[]> resultsOut) {
		assert resultsOut != null;
		long[] lastRes = checkLastEntry(lastNode, minTS, curLen);

		if (lastRes == null)
			return;

		resultsOut.add(lastRes);
	}

	private List<long[]> checkLastNode(final POHNode lastNode, final long minTS, final long curLen) {
		long[] lastRes = checkLastEntry(lastNode, minTS, curLen);
		if (lastRes == null)
			return null;

		List<long[]> res = new ArrayList<>(1);
		res.add(lastRes);
		return res;
	}

	private long[] checkLastEntry(final POHNode lastNode, final long minTS, final long curLen) {
		if (curLen%2 == 0 || lastNode.maxTS() >= minTS)
			return null;

		int lastEntryIndex = (int)((curLen/2) % entriesPerNode);

		return new long[]{lastNode.getSegmentInFile(lastEntryIndex)};
	}

	int lastNodeIndex(final long curLen, final int entriesPerNode) {
		//Length of one entry is 2. So ceil(curLen/2) gives the total number of entries, including the last half open entry
		// ceil( ceil(curLen/2) / entriesPerNode)
		return (int) ((((curLen+1)>>>1)+(entriesPerNode-1)) /entriesPerNode);
	}

	/**
	 * Find the right most (findLast=true) or the left most (findLast=false) node number that covers the ts in its tree rooted at the node
	 *
	 * @param ts
	 * @param findLast
	 * @return
	 */
	private int findNode(long ts, boolean findLast, int lastNodeIdx) {
		if (ts < nodes.get(1).minTS()) //if the first index entry is larger than the ts, i.e., the given range is lower than data
			return -1;

		int curNode = findRootNode(ts, lastNodeIdx);

		if (curNode <= 0) //No results found
			return -1;

		POHNode node = nodes.get(curNode);
		while(node.height() > 0) {
			if (node.entryMinTS() <= ts)
				break;

			if (findLast)
				curNode = node.findLastChild(ts);
			else
				curNode = node.findFirstChild(ts);

			node = nodes.get(curNode);
		}

		return curNode;
	}

	/**
	 * Find the right most root node that has minChildTS less than or equal to the given timestamp
	 * @param ts
	 * @return
	 */
	private int findRootNode(final long ts, final int lastNodeIdx) {
		if (ts < nodes.get(1).minTS()) //if the first index entry is larger than the ts, i.e., the given range is lower than data
			return -1;

		int curNode = lastNodeIdx; // Begin with the last node

		// System.out.printf("ts=%d, lastMin=%d, lastMax=%d\n", ts, nodes.get(curNode).minTS(), nodes.get(curNode).maxTS());

		// First traverse the root nodes up to the left-most tree
		while(curNode > 0) { // until we have explored all the root nodes; nodes[0] is null.
			// Move to the root of the tree on the left

			POHNode node = nodes.get(curNode);

			if (node.minTS() <= ts)
				break;

			curNode = curNode - nodesCountTable[node.height()];

		}

		return  curNode;
	}

	/**
	 * @param parentNodeNum
	 * @param childNumber
	 * @return
	 */
	private int nthChild(int parentNodeNum, int parentHeight, int childNumber) {
		assert parentHeight > 0 : "parentHieght must be greater than 0; parentHeight="+parentHeight;
		assert childNumber > 0;

		if (childNumber == childrenPerNode)
			return parentNodeNum - 1;

		return parentNodeNum - 1 - (childrenPerNode - childNumber)*(nodesCountTable[nodes.get(parentNodeNum-1).height()]);
	}

	/**
	 * Height of the node in the post-order heap
	 * @param nodeNumber
	 * @return
	 */
	private int heightOfNode(int nodeNumber) {
		int testHeight = heightOfRoot(nodeNumber);
		int totalNodesSubTree = nodesCountTable[testHeight];

		if (nodeNumber == totalNodesSubTree)
			return testHeight;

		return nodes.get(nodeNumber - totalNodesSubTree).height();
	}

	/**
	 * Total number of nodes in a k-ary tree of the given height
	 * @param height
	 * @return
	 */
	private int totalNodesKAryTree(int height) {
		// s(i, k) = k^(height+1)-1 / (k-1)  where k is the branching factor
		return (int)((Math.pow(childrenPerNode, (height+1))-1)/(childrenPerNode-1));
	}

	/**
	 * Height of the root node in a perfect k-ary tree that has total numNodes nodes in the tree.
	 *
	 * Depending on the number of nodes given and the branching factor, the returned height can be
	 * correct or wrong.
	 *
	 * @param numNodes
	 * @return
	 */
	private int heightOfRoot(int numNodes) {
		// h(i, k) = floor( log_k( nodeNum*(k-1) + 1 ) ) - 1  where k is the branching factor
		return (int)(Math.log(numNodes*(childrenPerNode-1) + 1)/logBase) - 1;
	}

	/**
	 * @return Number of nodes in the index
	 */
	public int size() {
		return nodes.size()-1; //As we added a dummy value in the nodes array
	}

	public void print() {
		for (POHNode node: nodes) {
			if (nodes != null)
				System.out.printf("%d,%d\n",node.nodeNumber(), node.height());
		}
	}
}
