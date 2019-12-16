package kawkab.fs.core.index.poh;

import kawkab.fs.core.ApproximateClock;
import kawkab.fs.core.Cache;
import kawkab.fs.core.IndexNodeID;
import kawkab.fs.core.LocalStoreManager;
import kawkab.fs.core.exceptions.IndexBlockFullException;
import kawkab.fs.core.exceptions.KawkabException;
import kawkab.fs.core.timerqueue.DeferredWorkReceiver;
import kawkab.fs.core.timerqueue.TimerQueueIface;
import kawkab.fs.core.timerqueue.TimerQueueItem;
import kawkab.fs.utils.LatHistogram;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Nodes in a post-order heap are created in the post-order, i.e., the parent node is created after the children. It is
 * an implicit data structure.
 *
 * The class supports only one modifier thread. The concurrent modifiers should synchronized externally.
 */

public class PostOrderHeapIndex implements DeferredWorkReceiver<POHNode> {
	// Persistent variables, persisted in the inode
	private long inumber;
	private final LocalStoreManager localStore = LocalStoreManager.instance();
	private final TimerQueueIface timerQ;
	private volatile TimerQueueItem<POHNode> acquiredNode;
	private static final ApproximateClock clock = ApproximateClock.instance();
	private static final int bufferTimeOffsetMs = 1; //Giving some time for buffering

	private final double logBase;
	private ConcurrentHashMap<Integer, POHNode> nodes;	//This is an append-only list. The readers should read but not modify the list. Only a single writer should append new nodes.

	// Configuration parameters
	private final int childrenPerNode; //Branching factor of the tree
	private final int entriesPerNode; //Number of index entries in each node
	private final int nodesPerBlock;

	// Number of timestamps in the index. The length divided by 2 shows the number of index entries in the index.
	// Moreover, if the length is an odd number, the last index entry is half complete.
	//TODO: We don't need this variable. We should calculate the length of index from the fileSize to avoid atomically
	// updating the fileSize and index length in the inode
	//private AtomicLong length = new AtomicLong(0);

	// nodesCountTable: A lookup table that contains the number of nodes in the perfect k-ary tree of height i, i>=0.
	// We use this table to avoid the complex math functions. Array index is the key, which is the node number.
	private int[] nodesCountTable; //TODO: This table should be final and static
	private int[] nodeHeightsTable;

	private final Cache cache;

	private int nodeSizeBytes;

	private LatHistogram loadLog;

	/**
	 *
	 * @param indexNodeSizeBytes
	 * @param percentEntriesPerNode
	 */
	public PostOrderHeapIndex(long inumber, int indexNodeSizeBytes, int nodesPerBlock, int percentEntriesPerNode, Cache cache, TimerQueueIface tq) {
		int entrySize = POHNode.entrySizeBytes();
		int childSize = POHNode.childSizeBytes();
		int headerBytes = POHNode.headerSizeBytes();

		nodeSizeBytes = indexNodeSizeBytes;
		entriesPerNode = (int) Math.ceil((indexNodeSizeBytes-headerBytes)*1.0*percentEntriesPerNode/100 / entrySize); //FIXME: We should get these values from POHONode
		childrenPerNode = (int) ((indexNodeSizeBytes-headerBytes-(entriesPerNode*entrySize))*1.0 / childSize);

		//System.out.printf("inumber: %d, indexNodeSize=%d, entriesPerNode=%d, childrenPerNode=%d, headerSize=%d, entrySize=%d, childSize=%d, %%entries=%d, nodesPerBlock=%d\n",
		//		inumber, indexNodeSizeBytes, entriesPerNode, childrenPerNode, headerBytes, entrySize, childSize, percentEntriesPerNode, nodesPerBlock);

		assert entriesPerNode > 0;
		assert childrenPerNode > 1;
		assert indexNodeSizeBytes >= entrySize*entriesPerNode + childSize*childrenPerNode;

		this.inumber = inumber;
		this.nodesPerBlock = nodesPerBlock;
		this.logBase = Math.log(childrenPerNode);
		this.cache = cache;
		this.timerQ = tq;

		System.out.printf("Per node index entries %d, pointers %d\n", entriesPerNode, childrenPerNode);

		nodes = new ConcurrentHashMap<>();
		//nodes.add(null); // Add a dummy value to match the node number with the array index. We do this to simplify the calculation of the index of the children of a node
		//currentNode = createNewNode(1);
		//nodes.add(currentNode);

		//TODO: Use a systematic way to get a good table size
		int count = 1000000;
		nodesCountTable = new int[count]; // We don't expect the height to grow large because of the large branching factor
		for (int i=0; i<nodesCountTable.length; i++) {
			nodesCountTable[i] = totalNodesKAryTree(i);
		}

		nodeHeightsTable = new int[count];
		for (int i=0; i<nodeHeightsTable.length; i++) {
			nodeHeightsTable[i] = -1;
		}
		for (int i=count-1; i>=0; i--) {
			nodeHeightsTable[i] = heightOfNode(i);
		}

		loadLog = new LatHistogram(TimeUnit.MICROSECONDS, "IndexNode load", 1, 5000);
	}

	/**
	 * Load the index from the local or the global store
	 *
	 * The length of the index must already be set.
	 */
	public void loadAndInit(final long indexLength) throws IOException, KawkabException {
		//long len = length.get();
		int nodesCount = (int)Math.ceil((indexLength + 1) / 2 / ((double)entriesPerNode)); // Ceil value of ( Total number of entries / entries per node )

		//System.out.printf("[POHI] Loading %d nodes, index length %d\n",nodesCount, len);

		for (int i=1; i<=nodesCount; i++) {
			acquireNode(i, true);
			//POHNode node = loadIndexNode(i);
			//nodes.put(i, node);
		}
	}

	private POHNode acquireNode(final int nodeNum, boolean loadFromPrimary) throws IOException, KawkabException {
		if (nodeNum == 0)
			return null;

		//System.out.printf("[POH] Acquire node %d\n", nodeNum);
		POHNode node = nodes.get(nodeNum);
		if (node == null) {
			synchronized (nodes) {
				node = nodes.get(nodeNum);
				if (node == null) {
					//System.out.printf("[POH] Acquiring from cache node %d\n", nodeNum);
					IndexNodeID id = new IndexNodeID(inumber, nodeNum);
					node = (POHNode) cache.acquireBlock(id);
					node.init(nodeNum, heightOfNode(nodeNum), entriesPerNode, childrenPerNode, nodeSizeBytes, nodesPerBlock);
					POHNode prev = nodes.put(nodeNum, node);
					assert prev == null;
				}
			}
		}

		loadLog.start();
		node.loadBlock(loadFromPrimary);
		loadLog.end();

		return node;
	}

	/**
	 * Create a file locally for the new block
	 *
	 * @param nodeID ID of the first node in the index block
	 * @return
	 */
	private void createNewBlock(IndexNodeID nodeID) throws IOException, InterruptedException {
		  //System.out.println("[POH] Creating new block: " + nodeID.localPath());

		localStore.createBlock(nodeID);
	}

	private POHNode createNewNodeCached(final int nodeNumber) throws IOException, KawkabException, InterruptedException {
		IndexNodeID nodeID = new IndexNodeID(inumber, nodeNumber);

		//System.out.println("[POH] Creating new node: " + nodeID);

		POHNode node = (POHNode) cache.acquireBlock(nodeID);
		node.init(nodeNumber, heightOfNode(nodeNumber), entriesPerNode, childrenPerNode, nodeSizeBytes, nodesPerBlock);
		node.initForAppend();
		setChildren(node, false);
		POHNode prev = nodes.put(nodeNumber, node); //No other thread (readers) can concurrently access this node because the filesize is not updated yet

		assert prev == null;

		if (nodeNumber % nodesPerBlock == 0 || nodeNumber == 1)
			createNewBlock(nodeID);

		return node;
	}

	/*private POHNode createNewNodeInMemory(final int nodeNumber) throws IOException, KawkabException {
		int height = heightOfNode(nodeNumber);
		IndexNodeID nodeID = new IndexNodeID(inumber, nodeNumber);
		POHNode node = new POHNode(nodeID);
		node.init(nodeNumber, height, entriesPerNode, childrenPerNode, nodeSizeBytes, nodesPerBlock);
		setChildren(node);
		return node;
	}*/

	private void setChildren(final POHNode node, boolean loadFromPrimary) throws IOException, KawkabException {
		if (node.height() > 0) {
			for (int i = 1; i <= childrenPerNode; i++) {
				POHNode child = acquireNode(nthChild(node.nodeNumber(), node.height(), i, loadFromPrimary), loadFromPrimary);
				try {
					node.appendChild(child);
				} catch (IndexBlockFullException e) {
					e.printStackTrace();
					return;
				}
			}
		}
	}

	/**
	 * Appends the timestamp of the first record in the segment number segmentInFile
	 *
	 * The function call to this function must precede the call to appendMaxTS.
	 *
	 * This function is not thread-safe.
	 *
	 * @param minTS timestamp
	 * @param segmentInFile segment number in the file
	 *
	 * @throws IOException
	 * @throws KawkabException
	 */
	public void appendMinTS(final long minTS, final long segmentInFile, final long curIndexLen) throws IOException, KawkabException {
		//TODO: check the arguments

		// Current node should not be null.
		// If current node is full,
		// 		create a new node,
		// 		set the new node to the current node
		// 		update the pointers
		//		insert the new index value
		// Add the new node in the nodes array

		//long curLength = length.get();

		//System.out.printf("[POH] appendMinTS: segInFile=%d, indexLen=%d\n", segmentInFile, curIndexLen);

		assert (curIndexLen % 2) == 0;

		boolean lastNodeFull = (curIndexLen % (entriesPerNode*2)) == 0;

		//System.out.printf("[POH] lastNodeFull=%b, curIdxLen=%d, entriesPerNodd=%d\n", lastNodeFull, curIndexLen, entriesPerNode);

		if (lastNodeFull) { //If last node is full
			try {
				int nodeNum = (int)(curIndexLen/2/entriesPerNode) +1;
				POHNode node = createNewNodeCached(nodeNum);
				acquiredNode = new TimerQueueItem<>(node, this);
			} catch (IOException | KawkabException | InterruptedException e) {
				e.printStackTrace();
				return;
			}
		}

		if (acquiredNode == null || !timerQ.tryDisable(acquiredNode)) {
			// FIXME: Note the variable overflow in curIndexLen+1
			int lastNode = (int)Math.ceil((curIndexLen + 1) / 2 / ((double)entriesPerNode)); //ceil(entriesInIndex)/(entriesPerNode) gives the ceil value;
			POHNode node = acquireNode(lastNode, false);
			acquiredNode = new TimerQueueItem<>(node, this);
		}

		POHNode currentNode = acquiredNode.getItem();

		try {
			currentNode.appendEntryMinTS(minTS, segmentInFile);
		} catch (IndexBlockFullException e) {
			e.printStackTrace();
		}

		timerQ.enableAndAdd(acquiredNode, clock.currentTime()+bufferTimeOffsetMs);

		//System.out.println("Min: " + minTS + ", seg: " + segmentInFile);
	}

	/**
	 * Appends the timestamp of the last record in the segment number segmentInFile. This function must be called
	 * after the corresponding appendMinTS
	 *
	 * segmentInFile must match with the last index entry's segmentInFile that was provided through appendMinTS function call.
	 *
	 * @param maxTS timestamp
	 * @param segmentInFile segment number in the file
	 *
	 * @throws IOException
	 * @throws KawkabException
	 */
	public void appendMaxTS(final long maxTS, final long segmentInFile, final long curIndexLen) throws IOException, KawkabException {
		//long curLength = length.get();

		//System.out.printf("[POH] appendMaxTS: segInFile=%d, indexLen=%d\n", segmentInFile, curIndexLen);

		assert curIndexLen % 2 == 1 : "Index length is incorrect: " + curIndexLen;

		if (acquiredNode == null || !timerQ.tryDisable(acquiredNode)) {
			int lastNode = (int)Math.ceil((curIndexLen + 1) / 2 / ((double)entriesPerNode));
			POHNode node = acquireNode(lastNode, false);
			acquiredNode = new TimerQueueItem<>(node, this);
		}

		POHNode currentNode = acquiredNode.getItem();

		currentNode.appendEntryMaxTS(maxTS, segmentInFile);

		timerQ.enableAndAdd(acquiredNode, clock.currentTime()+bufferTimeOffsetMs);

		if (currentNode.isFull()) {
			acquiredNode = null;
		}

		//System.out.println("\tMax: " + maxTS + ", seg: " + segmentInFile);
	}

	public void appendIndexEntry(final long minTS, final long maxTS, final long segmentInFile, final long curIndexLen) throws IOException, KawkabException {
		assert minTS <= maxTS;

		assert (curIndexLen % 2) == 0 : "Index length is incorrect: " + curIndexLen;

		//long curLength = length.get();

		//System.out.printf("[POH] appendIndexEntry: segInFile=%d, indexLen=%d\n", segmentInFile, curIndexLen);

		if ((curIndexLen % (entriesPerNode*2)) == 0) { //If last node is full
			try {
				int nodeNum = (int)(curIndexLen/2/entriesPerNode) +1;
				POHNode node = createNewNodeCached(nodeNum);
				acquiredNode = new TimerQueueItem<>(node, this);
			} catch (IOException | KawkabException | InterruptedException e) {
				e.printStackTrace();
				return;
			}
		}

		if (acquiredNode == null || !timerQ.tryDisable(acquiredNode)) {
			int lastNode = (int)Math.ceil((curIndexLen + 1) / 2 / ((double)entriesPerNode));
			POHNode node = acquireNode(lastNode, false);
			acquiredNode = new TimerQueueItem<>(node, this);
		}

		POHNode currentNode = acquiredNode.getItem();

		try {
			currentNode.appendEntry(minTS, maxTS, segmentInFile);
		} catch (IndexBlockFullException e) {
			e.printStackTrace();
		}

		timerQ.enableAndAdd(acquiredNode, clock.currentTime()+bufferTimeOffsetMs);

		if (currentNode.isFull()) {
			acquiredNode = null;
		}
	}

	/**
	 * Searches the most recent segment that has the timestamp.
	 *
	 * @param ts timestamp to find
	 *
	 * @return the segment number in file that contains ts, or -1 if no entry found
	 */
	public long findHighest(final long ts, final long indexLength, boolean loadFromPrimary) throws IOException, KawkabException {
		//long curLen = length.get();
		int lastNodeIdx = lastNodeIndex(indexLength, entriesPerNode);

		System.out.printf("[POH] findHighest %d, index len %d, num nodes %d, lastNodeIdx=%d\n", ts, indexLength, nodes.size(), lastNodeIdx);

		//System.out.println();

		POHNode curNode = findNode(ts, true, lastNodeIdx, loadFromPrimary);

		if (curNode == null)
			return -1;

		return curNode.findLastEntry(ts);
	}

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
	public List<long[]> findAllMinBased(final long minTS, final long maxTS, final long indexLen, final boolean loadFromPrimary) throws IOException, KawkabException {
		// Find the right most node that has maxTS
		// Traverse from that node to the left until the first value smaller than minTS is reached

		System.out.printf("[POH] findAllMinBased: indexLen=%d\n", indexLen);

		assert minTS <= maxTS;

		//long curLen = length.get();
		int lastNodeIdx = lastNodeIndex(indexLen, entriesPerNode);
		POHNode curNode = findNode(maxTS, true, lastNodeIdx, loadFromPrimary);

		if (curNode == null)
			return null;

		List<long[]> results = new ArrayList<>();

		while(curNode != null) {
			long[] res = curNode.findAllEntriesMinBased(minTS, maxTS);

			assert res != null;

			results.add(res);

			if (curNode.entryMinTS() < minTS)
				break;

			//curNode--;
			curNode = acquireNode(curNode.nodeNumber()-1, loadFromPrimary);
		}

		return results;
	}

	/**
	 * This function returns the segment numbers of all the segments that have minTS and maxTS inclusively.
	 *
	 * @param minTS
	 * @param maxTS
	 * @return
	 */
	public List<long[]> findAll(final long minTS, final long maxTS, final long indexLength, final boolean loadFromPrimary) throws IOException, KawkabException {
		// Moving to the roots of the trees on the left, find the right-most root node N that has maxTS between minChildTS and maxEntryTS
		// Traverse the tree rooted at N until the node K that has the maxTS between minEntryTS and maxEntryTS
		// While traversing down the tree, select the right most child that has minChildTS less than or equal to maxTS
		// Starting from the node K, add K.findAll(minTS, maxTS), and keep moving to the lower node numbers until
		// the node has maxEntryTS less than minTS.

		assert minTS <= maxTS : String.format("minTS (%d) is not <= maxTs (%d)", minTS, maxTS);

		//System.out.printf("[POH] findAll b/w %d and %d, index len %d, num nodes %d\n", minTS, maxTS, indexLength, nodes.size());

		//long curLen = length.get();

		int lastNodeIdx = lastNodeIndex(indexLength, entriesPerNode);

		POHNode curNode = findNode(maxTS, true, lastNodeIdx, loadFromPrimary);

		if (curNode == null) {
			POHNode lastNode = acquireNode(lastNodeIdx, loadFromPrimary);
			return checkLastNode(lastNode, minTS, indexLength);
		}

		List<long[]> results = new ArrayList<>();
		if (curNode.nodeNumber() == lastNodeIdx) {
			//POHNode lastNode = acquireNode(lastNodeIdx, loadFromPrimary);
			long[] res = checkLastEntry(curNode, minTS, indexLength);
			if (res != null)
				results.add(res);
		}

		while(curNode != null) {
			long[] res = curNode.findAllEntries(minTS, maxTS);

			if (res == null) { //This can happen if the given range is larger than the last index entry
				break;
			}

			results.add(res);

			if (curNode.entryMinTS() < minTS)
				break;

			curNode = acquireNode(curNode.nodeNumber()-1, loadFromPrimary);
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
		return (int) ((((curLen+1)>>>1)+(entriesPerNode-1)) / entriesPerNode);
	}

	/**
	 * Find the right most (findLast=true) or the left most (findLast=false) node number that covers the ts in its tree rooted at the node
	 *
	 * @param ts
	 * @param findLast
	 * @return
	 */
	private POHNode findNode(long ts, boolean findLast, int lastNodeIdx, boolean loadFromPrimary) throws IOException, KawkabException {
		//if (ts < acquireNode(1, loadFromPrimary).minTS()) //if the first index entry is larger than the ts, i.e., the given range is lower than data
		//	return null;

		//long t1 = System.currentTimeMillis();

		int curNode = findRootNode(ts, lastNodeIdx, loadFromPrimary);

		//long t2 = System.currentTimeMillis();

		if (curNode <= 0) //No results found
			return null;

		//LatHistogram ls1 = new LatHistogram(TimeUnit.MICROSECONDS, "dbg1", 100, 100000);
		//LatHistogram ls2 = new LatHistogram(TimeUnit.MICROSECONDS, "dbg2", 100, 100000);

		POHNode node = acquireNode(curNode, loadFromPrimary);
		while(node.height() > 0) {
			if (node.entryMinTS() <= ts)
				break;

			if (findLast) {
				//ls1.start();
				curNode = node.findLastChild(ts);
				//ls1.end();
			} else {
				//ls2.start();
				curNode = node.findFirstChild(ts);
				//ls2.end();
			}

			node = acquireNode(curNode, loadFromPrimary);
			//ls1.printStats();
			//ls2.printStats();
		}

		//long t3 = System.currentTimeMillis();

		//System.out.printf("t1=%d, t2=%d\n", t2-t1, t3-t2);

		return node;
	}

	/**
	 * Find the right most root node that has minChildTS less than or equal to the given timestamp
	 * @param ts
	 * @return
	 */
	private int findRootNode(final long ts, final int lastNodeIdx, boolean loadFromPrimary) throws IOException, KawkabException {
		//System.out.printf("[POH] findRootNode: ts=%d, lastMin=%d, lastMax=%d\n", ts, nodes.get(curNode).minTS(), nodes.get(curNode).maxTS());
		//if (ts < acquireNode(1, loadFromPrimary).minTS()) //if the first index entry is larger than the ts, i.e., the given range is lower than data
		//	return -1;

		int curNode = lastNodeIdx; // Begin with the last node

		// First traverse the root nodes up to the left-most tree
		while(curNode > 0) { // until we have explored all the root nodes; nodes[0] is null.
			// Move to the root of the tree on the left

			POHNode node = acquireNode(curNode, loadFromPrimary);

			//System.out.printf("[POH] findRootNode: ts=%d, node=%s, minTS=%d, maxTS=%d\n", ts, node.nodeNumber(), node.minTS(), node.maxTS());

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
	private int nthChild(int parentNodeNum, int parentHeight, int childNumber, boolean loadFromPrimary) throws IOException, KawkabException {
		assert parentHeight > 0 : "parentHeight must be greater than 0; parentHeight="+parentHeight;
		assert childNumber > 0;

		if (childNumber == childrenPerNode)
			return parentNodeNum - 1;

		return parentNodeNum - 1 - (childrenPerNode - childNumber)*(nodesCountTable[acquireNode(parentNodeNum-1, loadFromPrimary).height()]);
	}

	/**
	 * Height of the node in the post-order heap
	 * @param nodeNumber
	 * @return
	 */
	private int heightOfNode(int nodeNumber) {
		if (nodeHeightsTable[nodeNumber] >= 0)
			return nodeHeightsTable[nodeNumber];

		if (nodeNumber <= childrenPerNode)
			return 0;

		int testHeight = heightOfRoot(nodeNumber);
		int totalNodesSubTree = nodesCountTable[testHeight];

		if (nodeNumber == totalNodesSubTree)
			return testHeight;

		int prevNode = nodeNumber - totalNodesSubTree;
		int prevNodeHeight = heightOfNode(prevNode);
		nodeHeightsTable[prevNode] = prevNodeHeight;
		return prevNodeHeight;

		//return acquireNode(nodeNumber - totalNodesSubTree, loadFromPrimary).height();
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
	 * correct or incorrect. This function is for internal use to find the height of a given node. See
	 * the heigtOfNode() function.
	 *
	 * @param numNodes
	 * @return
	 */
	private int heightOfRoot(int numNodes) {
		// h(i, k) = floor( log_k( nodeNum*(k-1) + 1 ) ) - 1  where k is the branching factor
		//https://stackoverflow.com/questions/3305059/how-do-you-calculate-log-base-2-in-java-for-integers
		return (int)((Math.log(numNodes*(childrenPerNode-1) + 1)/logBase) - 1 + 1e-10); // +1e-10 is avoid rounding-off error. See the above link.
	}

	/**
	 * @return Number of nodes in the index
	 */
	public int size(long indexLen) {
		return lastNodeIndex(indexLen, entriesPerNode) + 1;
	}

	public void print() {
		int size = nodes.size();
		for (int i=size; i>0; i--) {
			POHNode node = nodes.get(i);
			System.out.printf("%d,%d\n",node.nodeNumber(), node.height());
		}
	}

	public void shutdown() throws KawkabException {
		//timerQ.waitUntilEmpty();

		if (acquiredNode != null && timerQ.tryDisable(acquiredNode)) {
			deferredWork(acquiredNode.getItem());
		}

		for (POHNode node : nodes.values()) {
			if (node != null) {
				cache.releaseBlock(node.id());
			}
		}

		nodes.clear();
		nodes = null;
	}

	public void flush() throws KawkabException {
		if (acquiredNode != null && timerQ.tryDisable(acquiredNode)) {
			deferredWork(acquiredNode.getItem());
		}

		for (POHNode node : nodes.values()) {
			if (node != null) {
				cache.releaseBlock(node.id());
			}
		}

		nodes.clear();
	}

	public void printStats() {
		loadLog.printStats();

	}

	public void resetStats() {
		loadLog.reset();
	}

	@Override
	public void deferredWork(POHNode node) {
		try {
			localStore.store(node);
		} catch (KawkabException e) {
			e.printStackTrace();
		}
	}
}
