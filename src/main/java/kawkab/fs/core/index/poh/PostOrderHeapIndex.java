package kawkab.fs.core.index.poh;

import kawkab.fs.core.exceptions.IndexBlockFullException;
import kawkab.fs.core.index.FileIndex;

import java.util.ArrayList;
import java.util.List;

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
		nodes.add(null); // Add a dummy value to match the node number with the array index
		nodes.add(currentNode);

		//TODO: Use a systematic way to get a good table size
		nodesCountTable = new int[51]; // We don't expect the height to grow large because of the large branching factor
		for (int i=0; i<nodesCountTable.length; i++) {
			nodesCountTable[i] = totalNodesKAryTree(i);
		}
	}

	/**
	 * Appends the timestamps to the index
	 *
	 * This function must be called by a single thread.
	 *
	 * @param timestamp timestamp in the data segment
	 * @param segmentInFile Segment number in the file that has the record with the timestamp
	 */
	@Override
	public void append(long timestamp, long segmentInFile) {
		//TODO: check the arguments

		// Current node should not be null.
		// If current node is full,
		// 		create a new node,
		// 		set the new node to the current node
		// 		update the pointers
		//		insert the new index value
		// Add the new node in the nodes array

		if (currentNode.isFull()) {
			int nodeNumber = currentNode.nodeNumber()+1;
			int height = heightOfNode(nodeNumber);

			currentNode = new POHNode(nodeNumber, height, entriesPerNode, childrenPerNode);

			if (height > 0) {
				for (int i = 1; i <= childrenPerNode; i++) {
					POHNode child = nodes.get(nthChild(nodeNumber, height, i));
					try {
						currentNode.appendChild(child);
					} catch (IndexBlockFullException e) {
						e.printStackTrace();
						return;
					}
				}
			}

			nodes.add(currentNode);
		}

		try {
			currentNode.appendEntry(timestamp, segmentInFile);
		} catch (IndexBlockFullException e) {
			e.printStackTrace();
		}
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
		int curNode = findNode(ts, true);

		if (curNode == -1)
			return -1;

		return nodes.get(curNode).findLastEntry(ts);
	}

	/**
	 * Find all data segments that have timestamps between minTS and maxTS inclusively
	 * @param minTS
	 * @param maxTS
	 * @return null if no such segment is found
	 */
	@Override
	public List<long[]> findAll(final long minTS, final long maxTS) {
		// Find the right most node that has maxTS
		// Traverse from that node to the left until the first value smaller than minTS is reached

		assert minTS <= maxTS;

		int curNode = findNode(maxTS, true);

		if (curNode == -1)
			return null;

		List<long[]> results = new ArrayList<>();

		// Traver each node from the rightMostNode until the node.entryMinTS()
		while(curNode > 0) {
			POHNode node = nodes.get(curNode);

			long[] res = node.findAllEntries(minTS, maxTS);

			if (res == null)
				assert false;

			results.add(res);

			if (node.entryMinTS() < minTS)
				break;

			curNode--;
		}

		return results;
	}

	private int findNode(long maxTS, boolean findLast) {
		if (maxTS < nodes.get(1).minTS()) //if the first index entry is larger than the maxTS, i.e., the given range is lower than data
			return -1;

		int curNode = nodes.size()-1; // Begin with the last node
		POHNode node = nodes.get(curNode);

		System.out.printf("maxTS=%d, lastMin=%d, lastMax=%d\n", maxTS, node.minTS(), node.maxTS());

		// First traverse the root nodes up to the left-most tree
		while(curNode > 0) { // until we have explored all the root nodes; nodes[0] is null.
			// Move to the root of the tree on the left

			if (node.minTS() <= maxTS)
				break;

			curNode = curNode - nodesCountTable[node.height()];
			node = nodes.get(curNode);
		}

		if (curNode == 0) //No results found
			return -1;

		while(node.height() > 0) {
			if (node.entryMinTS() <= maxTS)
				break;

			if (findLast)
				curNode = node.findLastChild(maxTS);
			else
				curNode = node.findFirstChild(maxTS);

			node = nodes.get(curNode);
		}

		return curNode;
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
