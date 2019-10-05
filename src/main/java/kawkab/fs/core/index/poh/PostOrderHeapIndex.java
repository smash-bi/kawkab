package kawkab.fs.core.index.poh;

import kawkab.fs.core.exceptions.IndexBlockFullException;

import java.util.ArrayList;

/**
 * Nodes in a post-order heap are created in the post-order, i.e., the parent node is created after the children. It is
 * an implicit data structure.
 *
 * The class supports only one modifier thread. The concurrent modifiers should synchronized externally.
 */

public class PostOrderHeapIndex {
	private final double logBase;
	private ArrayList<POHNode> nodes;	//This is an append-only list. The readers should read but not modify the list. The sole writer can a[[emd new nodes.

	// Configuration parameters
	private final int childrenPerNode; //Branching factor of the tree
	private final int entriesPerNode; //Number of index entries in each node
	private POHNode currentNode;

	//TODO: This table should final and static
	private int[] nodesCountTable; // Number of nodes in the perfect k-ary tree of height i, i>=0. We use this table to avoid the complex math functions

	/**
	 *
	 * @param childrenPerNode Number of children per node, which is equivalent to the number of pointers in each node
	 * @param entriesPerNode Number of index entries per node
	 */
	public PostOrderHeapIndex(final int entriesPerNode, final int childrenPerNode) {
		this.entriesPerNode = entriesPerNode;
		this.childrenPerNode = childrenPerNode;

		logBase = Math.log(childrenPerNode);

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
	 * @param minTS minimum timestamp in the data segment
	 * @param maxTS maximum timestamp in the data segment
	 * @param segInFile Segment number in the file that has minTS and maxTS
	 */
	public void insert(long minTS, long maxTS, long segInFile) {
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
			currentNode.appendEntry(minTS, maxTS, segInFile);
		} catch (IndexBlockFullException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Searches the timestamp in the index.
	 *
	 * @param ts timestamp to find
	 * @param findLargest Whether to find the largest segment or the smallest segment that has ts
	 *
	 * @return the segment number in file that contains ts
	 */
	public long find(long ts, boolean findLargest) {
		//TODO: check the arguments

		// Start from the last node and find the left-most node that has the range. FIXME: We are doing linear search here. Can we do binary search?

		POHNode targetNode = null;
		int curNode = currentNode.nodeNumber(); // Begin with the last node

		if (currentNode.maxTS() < ts || nodes.get(1).minTS() > ts)
			return -1;

		// First find the target root node
		while(curNode > 0) { // Node 0 is null; if we have explored all the root nodes
			POHNode node = nodes.get(curNode);

			int res = node.compare(ts); // Check if the current node has ts
			if (res == 0) { //This node has the target timestamp
				targetNode = node;
				if (findLargest) // If we are finding the largest index entry that has the timestamp, we should stop at the first node that has ts
					break;
			} else if (res < 0) { //If the current node's maxTS is smaller than ts
				break; // No need to further search in the left as all the left nodes have smaller timestamps than the given ts
			}

			// Move to the right-most tree on the left
			curNode = curNode - nodesCountTable[node.height()];
		}

		if (targetNode == null) // If no node is found
			return -1; // The ts is not found

		// Now traverse the tree down until we reach the target node or the leaf node
		while (targetNode.height() > 0) {
			// Now ts is either in the index entries of the targetNode or its children.
			// If ts is larger than the maxTS of the right-most child, the ts is in the targetNode's index entries.
			// Otherwise ts is among the children.

			// if the children have smaller timestamps than ts
			if (targetNode.nthChild(childrenPerNode).maxTS() < ts)
				break; // stop here because all the nodes left to this node have smaller timestamps

			int childNodeNum = -1;
			if (findLargest) {
				// Find the right-most child that has ts
				childNodeNum = targetNode.findLastChild(ts);
			} else {
				// Find the left-most child that has ts
				childNodeNum = targetNode.findFirstChild(ts);
			}

			// Visit and explore the child that has ts
			targetNode = nodes.get(childNodeNum);
		}

		if (findLargest)
			return targetNode.findLastEntry(ts);
		else
			return targetNode.findFirstEntry(ts);
	}

	/**
	 * @param ts
	 * @return null if ts is not found, otherwise returns the lists of the list of segment numbers in the descending order
	 */
	/*public long[][] findAll(long ts) {
		// We return array list because we don't know how many index nodes have the target ts.

		int curNode = currentNode.nodeNumber(); // Begin with the last node

		if (currentNode.maxTS() < ts || nodes.get(1).minTS() > ts)
			return null;

		POHNode leftMostNode = null;
		int rightIdx = -1; //Node number/index of the right-most node that has ts

		// Traverse all the root nodes that have the ts
		while(curNode > 0) { // Node 0 is null; if we have explored all the root nodes
			POHNode node = nodes.get(curNode);
			int nodeNum = node.nodeNumber();

			int res = node.compare(ts); // Check if the current node has ts

			if (res < 0) { //If the current node's maxTS is smaller than ts
				break; // No need to further search in the left as all the left nodes have smaller timestamps than the given ts
			}

			if (res == 0) { //This node's children or the index entries has the target timestamp
				leftMostNode = node;

				if (rightIdx < nodeNum && node.entryMinTS() <= ts) { // if the ts is greater than the first index entry's minTS
					rightIdx = nodeNum; //This is the right-most node that has ts
				}
			}

			// Move to the right-most tree on the left
			curNode = curNode - nodesCountTable[node.height()];
		}

		if (leftMostNode == null) //If no node is found
			return null;

		// Now traverse the tree down until we reach the target node or the leaf node
		while (leftMostNode.height() > 0) {
			// Now ts is either in the index entries of the targetNode or its children.
			// If ts is larger than the maxTS of the right-most child, ts is in the targetNode's index entries.
			// Otherwise ts is among the children.

			// if the children have smaller timestamps than ts
			if (leftMostNode.nthChild(childrenPerNode).maxTS() < ts)
				break; // stop here because all the nodes left to this node have smaller timestamps

			// Find the left-most child that has ts
			int childNodeNum = leftMostNode.findFirstChild(ts);

			// Visit and explore the child that has ts
			leftMostNode = nodes.get(childNodeNum);
		}

		int leftIdx = leftMostNode.nodeNumber();
		if (rightIdx == -1) //If ts was not in any other node
			rightIdx = leftIdx;

		long[][] results = new long[rightIdx-leftIdx+1][];
		for(int i=0; i<results.length; i++) {
			results[i] = nodes.get(rightIdx-i).findAllEntries(ts);
		}

		return results;
	}*/

	/**
	 * Find all data segments that have timestamps between minTS and maxTS inclusively
	 * @param minTS
	 * @param maxTS
	 * @return null if no such segment is found
	 */
	public long[][] findAll(final long minTS, final long maxTS) {
		// Find the right most node that has maxTS
		// Find the left most node that has the minTS

		assert minTS <= maxTS;

		POHNode firstNode = nodes.get(1); // Nodes start from index 1
		POHNode lastNode = nodes.get(nodes.size()-1);

		if (lastNode.maxTS() < minTS || firstNode.minTS() > maxTS)
			return null;

		int curNode = lastNode.nodeNumber(); // Begin with the last node

		int leftIdx = curNode+1;
		int rightIdx = -1; //Node number/index of the right-most node that has ts
		POHNode leftMostNode = null;
		POHNode rightMostNode = null;

		if (minTS < firstNode.minTS()) { //If the minTS is lower than the first entry
			leftMostNode = firstNode;
			leftIdx = firstNode.nodeNumber();
		}

		if (maxTS > lastNode.maxTS()) { //If the maxTS is higher than the last entry
			rightMostNode = lastNode;
			rightIdx = rightMostNode.nodeNumber();
		}

		// First traverse the root nodes up to the left-most tree
		while(curNode > 0) { // Node 0 is null; if we have explored all the root nodes
			POHNode node = nodes.get(curNode);

			if (node.maxTS() < minTS) //if the node has lower timestamps, all other nodes have lower timestamps. So stop search here.
				break;

			if (rightMostNode == null && node.entryMinTS() <= maxTS) { // if the maxTS is withing the range of entries
				rightIdx = curNode;
				rightMostNode = node;
			}

			if (leftIdx > curNode && node.compare(minTS) >= 0) {
				leftIdx = curNode;
				leftMostNode = node;
			}

			// Move to the right-most tree on the left
			curNode = curNode - nodesCountTable[node.height()];
		}

		// If we haven't found the right-most node yet, check if should traverse any other branch than the left-most
		if (rightMostNode == null) { // if the maxTS is within the range of the current node's entries
			rightMostNode = leftMostNode; //We are at the root of the left-most sub-tree that has ts

			while(rightMostNode.height() > 0) {
				curNode = rightMostNode.nodeNumber();
				if (rightIdx < curNode && rightMostNode.entryMinTS() <= maxTS) {
					rightIdx = curNode;
					break;
				}

				rightMostNode = nodes.get(rightMostNode.findLastChild(maxTS));
			}
		}

		while(leftMostNode.height() > 0) {
			// Now ts is either in the index entries of the targetNode or its children.
			// If ts is larger than the maxTS of the right-most child, ts is in the targetNode's index entries.
			// Otherwise ts is among the children.

			// if the children have smaller timestamps than ts
			if (leftMostNode.nthChild(childrenPerNode).maxTS() < minTS)
				break; // stop here because all the nodes left to this node have smaller timestamps

			// Find the left-most child that has ts
			leftMostNode = nodes.get(leftMostNode.findFirstChild(minTS));
		}

		leftIdx = leftMostNode.nodeNumber();
		rightIdx = rightMostNode.nodeNumber();

		long[][] results = new long[rightIdx-leftIdx+1][];
		for(int i=0; i<results.length; i++) {
			results[i] = nodes.get(rightIdx-i).findAllEntries(minTS, maxTS);
			if (results[i] == null) //We conclude this because the index entries cannot overlap after a gap. So following is not possible entries: <1,5>, <7,9>, problem <8, 11>.
				return null;
		}

		return results;
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
