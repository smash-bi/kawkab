package kawkab.fs.core.index.poh;

import kawkab.fs.commons.Configuration;
import kawkab.fs.core.NullCache;
import kawkab.fs.core.exceptions.KawkabException;
import kawkab.fs.core.timerqueue.NullTimerQueue;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

public class PostOrderHeapIndexTest {
	@BeforeAll
	public static void initialize() throws IOException, InterruptedException, KawkabException {
		System.out.println("-------------------------------");
		System.out.println("- Initializing -");
		System.out.println("-------------------------------");

		int nodeID = Configuration.getNodeID();
		Properties props = Configuration.getProperties(Configuration.propsFileCluster);
		Configuration.configure(nodeID, props);
	}

	@Test
	public void rootHeightFuncTest() throws IllegalAccessException, NoSuchMethodException, InvocationTargetException {
		System.out.println("Test: rootHeightFuncTest");

		int epn = 1; // entries per node
		int cpn = 3; // children per node
		int nodeSize = epn*POHEntry.sizeBytes() + cpn*POHNode.childSizeBytes()  + POHNode.headerSizeBytes();
		int nodesPerBlock = Configuration.instance().nodesPerBlockPOH;
		PostOrderHeapIndex poh = new PostOrderHeapIndex(1, nodeSize, nodesPerBlock, 25, new NullCache(), new NullTimerQueue());

		assertEquals(0, heightOfRoot(poh, 3));
		assertEquals(1, heightOfRoot(poh, 4));
		assertEquals(2, heightOfRoot(poh, 13));
	}

	private int heightOfRoot(PostOrderHeapIndex poh, int numNodes) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
		Method method = PostOrderHeapIndex.class.getDeclaredMethod("heightOfRoot", int.class);
		method.setAccessible(true);
		return (int) method.invoke(poh, numNodes);
	}

	@Test
	public void totalNodesCountTest() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
		System.out.println("Test: rootHeightFuncTest");
		int epn = 1; // entries per node
		int cpn = 3; // children per node
		int nodeSize = epn*POHEntry.sizeBytes() + cpn*POHNode.childSizeBytes()  + POHNode.headerSizeBytes();
		int nodesPerBlock = Configuration.instance().nodesPerBlockPOH;
		PostOrderHeapIndex poh = new PostOrderHeapIndex(2, nodeSize, nodesPerBlock, 25, new NullCache(), new NullTimerQueue());
		assertEquals(4, totalNodesKAryTree(poh, 1));
		assertEquals(13, totalNodesKAryTree(poh, 2));
		assertEquals(40, totalNodesKAryTree(poh, 3));

		epn = 1; // entries per node
		cpn = 2; // children per node
		nodeSize = epn*POHEntry.sizeBytes() + cpn*POHNode.childSizeBytes() + POHNode.headerSizeBytes();
		nodesPerBlock = Configuration.instance().nodesPerBlockPOH;
		poh = new PostOrderHeapIndex(3, nodeSize, nodesPerBlock, 33, new NullCache(), new NullTimerQueue());
		assertEquals(3, totalNodesKAryTree(poh, 1));
		assertEquals(7, totalNodesKAryTree(poh, 2));
		assertEquals(15, totalNodesKAryTree(poh, 3));

		epn = 1; // entries per node
		cpn = 4; // children per node
		nodeSize = epn*POHEntry.sizeBytes() + cpn*POHNode.childSizeBytes() + POHNode.headerSizeBytes();
		nodesPerBlock = Configuration.instance().nodesPerBlockPOH;
		poh = new PostOrderHeapIndex(4, nodeSize, nodesPerBlock, 20, new NullCache(), new NullTimerQueue());
		assertEquals(5, totalNodesKAryTree(poh, 1));
		assertEquals(21, totalNodesKAryTree(poh, 2));
		assertEquals(85, totalNodesKAryTree(poh, 3));
	}

	private int totalNodesKAryTree(PostOrderHeapIndex poh, int height) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
		Method method = PostOrderHeapIndex.class.getDeclaredMethod("totalNodesKAryTree", int.class);
		method.setAccessible(true);
		return (int) method.invoke(poh, height);
	}

	@Test
	public void pohStructureTest() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, IOException, KawkabException {
		System.out.println("Test: pohStructureTest");

		int epn = 1; // entries per node
		int cpn = 3; // children per node
		int nodeSize = epn*POHEntry.sizeBytes() + cpn*POHNode.childSizeBytes() + POHNode.headerSizeBytes();
		int nodesPerBlock = Configuration.instance().nodesPerBlockPOH;
		PostOrderHeapIndex poh = new PostOrderHeapIndex(5, nodeSize, nodesPerBlock, 25, new NullCache(), new NullTimerQueue());

		int len = 0;
		for (int i=1; i<30; i++) {
			poh.appendIndexEntry(i, i, i, len);
			len += 2;
		}

		assertEquals(0, heightOfNode(poh, 1));
		assertEquals(0, heightOfNode(poh, 3));
		assertEquals(1, heightOfNode(poh, 4));
		assertEquals(1, heightOfNode(poh, 21));
		assertEquals(2, heightOfNode(poh, 26));
		assertEquals(0, heightOfNode(poh, 29));
	}

	private int heightOfNode(PostOrderHeapIndex poh, int nodeNumber) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
		Method method = PostOrderHeapIndex.class.getDeclaredMethod("heightOfNode", int.class, boolean.class);
		method.setAccessible(true);
		return (int) method.invoke(poh, nodeNumber, false);
	}

	@Test
	public void searchSmokeTest() throws IOException, KawkabException {
		System.out.println("Test: searchFirstTest");

		int epn = 1; // entries per node
		int cpn = 3; // children per node
		int nodeSize = epn*POHEntry.sizeBytes() + cpn*POHNode.childSizeBytes() + POHNode.headerSizeBytes();
		int nodesPerBlock = Configuration.instance().nodesPerBlockPOH;
		PostOrderHeapIndex poh = new PostOrderHeapIndex(6, nodeSize, nodesPerBlock, 25, new NullCache(), new NullTimerQueue());
		int indexLen = 0;
		for (int i=1; i<30; i++) {
			poh.appendIndexEntry(i, i, i, indexLen);
			indexLen += 2;
		}
		assertEquals(25, poh.findHighest(25, indexLen, true));
		assertEquals(19, poh.findHighest(19, indexLen, true));
		assertEquals(1, poh.findHighest(1, indexLen, true));

		assertEquals(29, poh.findHighest(30, indexLen, true));
		assertEquals(-1, poh.findHighest(0, indexLen, true));
	}

	@Test
	public void searchOverlappingTest() throws IOException, KawkabException {
		System.out.println("Test: searchOverlappingTest");

		int epn = 5; // entries per node
		int cpn = 3; // children per node
		int nodeSize = epn*POHEntry.sizeBytes() + cpn*POHNode.childSizeBytes() + POHNode.headerSizeBytes();
		int nodesPerBlock = Configuration.instance().nodesPerBlockPOH;
		PostOrderHeapIndex poh = new PostOrderHeapIndex(7, nodeSize, nodesPerBlock, 62, new NullCache(), new NullTimerQueue());
		int len = 0;
		for (int i=1; i<=100; i++) {
			poh.appendIndexEntry(i, i, i, len);
			len += 2;
		}

		assertEquals(2, poh.findHighest(2, len, true));
		assertEquals(3, poh.findHighest(3, len, true));

		assertEquals(100, poh.findHighest(102, len, true));
		assertEquals(-1, poh.findHighest(0, len, true));
	}

	@Test
	public void findAllRangeTest() throws IOException, KawkabException, InterruptedException {
		System.out.println("Test: findAllRangeTest");

		int epn = 3; // entries per node
		int cpn = 3; // children per node
		int nodeSize = epn*POHEntry.sizeBytes() + cpn*POHNode.childSizeBytes() + POHNode.headerSizeBytes();
		int nodesPerBlock = Configuration.instance().nodesPerBlockPOH;
		PostOrderHeapIndex poh = new PostOrderHeapIndex(8, nodeSize, nodesPerBlock, 50, new NullCache(), new NullTimerQueue());
		int len = 0;
		poh.appendMinTS(3, 1, len++);
		poh.appendMaxTS(3, 1, len++);
		poh.appendMinTS(5, 2, len++);
		poh.appendMaxTS(5, 2, len++);
		poh.appendMinTS(11, 3, len++);
		poh.appendMaxTS(11, 3, len++);

		poh.appendMinTS(18, 4, len++);
		poh.appendMaxTS(18, 4, len++);
		poh.appendMinTS(20, 5, len++);
		poh.appendMaxTS(20, 5, len++);
		poh.appendMinTS(25, 6, len++);
		poh.appendMaxTS(25, 6, len++);

		poh.appendMinTS(30, 7, len++);
		poh.appendMaxTS(30, 7, len++);
		poh.appendMinTS(30, 8, len++);
		poh.appendMaxTS(30, 8, len++);
		poh.appendMinTS(30, 9, len++);
		poh.appendMaxTS(30, 9, len++);

		poh.appendMinTS(35, 10, len++);
		poh.appendMaxTS(35, 10, len++);
		poh.appendMinTS(45, 11, len++);
		poh.appendMaxTS(45, 11, len++);
		poh.appendMinTS(50, 12, len++);
		poh.appendMaxTS(50, 12, len++);

		poh.appendMinTS(55, 13, len++);
		poh.appendMaxTS(55, 13, len++);
		poh.appendMinTS(65, 14, len++);
		poh.appendMaxTS(65, 14, len++);
		poh.appendMinTS(75, 15, len++);
		poh.appendMaxTS(75, 15, len++);

		poh.appendMinTS(85, 16, len++);
		poh.appendMaxTS(85, 16, len++);
		poh.appendMinTS(93, 17, len++);
		poh.appendMaxTS(93, 17, len++);
		poh.appendMinTS(99, 18, len++);
		poh.appendMaxTS(99, 18, len++);

		poh.appendMinTS(105, 19, len++);
		poh.appendMaxTS(105, 19, len++);

		long[][] resType = new long[][]{{}};

		assertArrayEquals(new long[][]{{15, 14, 13}, {12, 11, 10}, {9, 8, 7}, {6, 5, 4}, {3, 2, 1}}, poh.findAllMinBased(3, 77,len, true).toArray(resType));
		assertArrayEquals(new long[][]{{6, 5, 4}}, poh.findAllMinBased(20, 25,len, true).toArray(resType));

		assertArrayEquals(new long[][]{{9, 8, 7}, {6, 5, 4}}, poh.findAllMinBased(20, 30,len, true).toArray(resType));

		assertEquals(null, poh.findAllMinBased(1, 2,len, true)); //Out of lower limit
		assertArrayEquals(new long[][]{{19}}, poh.findAllMinBased(111, 112,len, true).toArray(resType)); //Out of upper limit
		assertArrayEquals(new long[][]{{13}}, poh.findAllMinBased(61, 63,len, true).toArray(resType)); //Out of range but b/w entries

		assertArrayEquals(new long[][]{{2, 1}}, poh.findAllMinBased(1, 8,len, true).toArray(resType)); // partially cover lower limit
		assertArrayEquals(new long[][]{{19}, {18, 17}}, poh.findAllMinBased(95, 111,len, true).toArray(resType)); // partially cover upper limit

		assertArrayEquals(new long[][]{{10}, {9, 8, 7}, {6, 5}}, poh.findAllMinBased(22, 42,len, true).toArray(resType)); // cover entries but fall across entries' ranges
		assertArrayEquals(new long[][]{{15, 14, 13}, {12, 11, 10}, {9}}, poh.findAllMinBased(32, 81,len, true).toArray(resType)); // cover entries but fall across entries' ranges and nodes
		assertArrayEquals(new long[][]{{15, 14, 13}, {12, 11}}, poh.findAllMinBased(47, 82,len, true).toArray(resType)); // cover entries from lower part
		assertArrayEquals(new long[][]{{17, 16}, {15}}, poh.findAllMinBased(82, 98,len, true).toArray(resType)); // cover entries from upper part

		assertArrayEquals(new long[][]{{9, 8, 7}, {6}}, poh.findAllMinBased(30, 30,len, true).toArray(resType)); // find all with same ts
	}

	@Test
	public void singleEntryTest() throws IOException, KawkabException, InterruptedException {
		System.out.println("Test: singleEntryTest");

		int epn = 3; // entries per node
		int cpn = 3; // children per node
		int nodeSize = epn*POHEntry.sizeBytes() + cpn*POHNode.childSizeBytes() + POHNode.headerSizeBytes();
		int nodesPerBlock = Configuration.instance().nodesPerBlockPOH;
		PostOrderHeapIndex poh = new PostOrderHeapIndex(9, nodeSize, nodesPerBlock, 50, new NullCache(), new NullTimerQueue());
		int len = 0;
		poh.appendMinTS(3, 1, len++);

		long[][] resType = new long[][]{{}};

		assertArrayEquals(new long[][]{{1}}, poh.findAllMinBased(1, 6,len, true).toArray(resType));
 		assertArrayEquals(new long[][]{{1}}, poh.findAllMinBased(1, 3,len, true).toArray(resType));
		assertArrayEquals(new long[][]{{1}}, poh.findAllMinBased(3, 3,len, true).toArray(resType));
		assertArrayEquals(new long[][]{{1}}, poh.findAllMinBased(3, 5,len, true).toArray(resType));
		assertNull(poh.findAllMinBased(1, 2,len, true));
		assertArrayEquals(new long[][]{{1}}, poh.findAllMinBased(6, 7,len, true).toArray(resType));
	}

	@Test
	public void exactMatchTest() throws IOException, KawkabException {
		System.out.println("Test: exactMatchTest");

		int epn = 3; // entries per node
		int cpn = 3; // children per node
		int nodeSize = epn*POHEntry.sizeBytes() + cpn*POHNode.childSizeBytes() + POHNode.headerSizeBytes();
		int nodesPerBlock = Configuration.instance().nodesPerBlockPOH;
		PostOrderHeapIndex poh = new PostOrderHeapIndex(10, nodeSize, nodesPerBlock, 50, new NullCache(), new NullTimerQueue());
		int len = 0;
		poh.appendIndexEntry(3, 3, 1, len); len += 2;
		poh.appendIndexEntry(5, 5, 2, len); len += 2;
		poh.appendIndexEntry(11, 11, 3, len); len += 2;

		poh.appendIndexEntry(18, 18, 4, len); len += 2;
		poh.appendIndexEntry(20, 20, 5, len); len += 2;
		poh.appendIndexEntry(25, 25, 6, len); len += 2;

		poh.appendIndexEntry(30, 30, 7, len); len += 2;
		poh.appendIndexEntry(30, 30, 8, len); len += 2;
		poh.appendIndexEntry(30, 30, 9, len); len += 2;

		poh.appendIndexEntry(35, 35, 10, len); len += 2;
		poh.appendIndexEntry(45, 45, 11, len); len += 2;
		poh.appendIndexEntry(50, 50, 12, len); len += 2;

		poh.appendIndexEntry(55, 55, 13, len); len += 2;
		poh.appendIndexEntry(65, 65, 14, len); len += 2;
		poh.appendIndexEntry(75, 75, 15, len); len += 2;

		poh.appendIndexEntry(85, 85, 16, len); len += 2;
		poh.appendIndexEntry(93, 93, 17, len); len += 2;
		poh.appendIndexEntry(99, 99, 18, len); len += 2;

		poh.appendIndexEntry(105, 105, 19, len); len += 2;

		assertEquals(-1, poh.findHighest(1,len, true)); //Out of lower limit

		assertEquals(9, poh.findHighest(30,len, true));

		assertEquals(12, poh.findHighest(50,len, true)); //Finding in an internal node
	}

	@Test
	public void sameMinMaxTest() throws IOException, KawkabException, InterruptedException {
		System.out.println("Test: sameMinMaxTest");

		int epn = 3; // entries per node
		int cpn = 3; // children per node
		int nodeSize = epn*POHEntry.sizeBytes() + cpn*POHNode.childSizeBytes() + POHNode.headerSizeBytes();
		int nodesPerBlock = Configuration.instance().nodesPerBlockPOH;
		PostOrderHeapIndex poh = new PostOrderHeapIndex(11, nodeSize, nodesPerBlock, 50, new NullCache(), new NullTimerQueue());
		int len = 0;
		poh.appendMinTS(3, 1, len++);
		poh.appendMaxTS(3, 1, len++);
		poh.appendMinTS(5, 2, len++);
		poh.appendMaxTS(5, 2, len++);
		poh.appendMinTS(11, 3, len++);
		poh.appendMaxTS(11, 3, len++);

		poh.appendIndexEntry(18, 18, 4, len); len += 2;
		poh.appendIndexEntry(20, 20, 5, len); len += 2;
		poh.appendIndexEntry(25, 25, 6, len); len += 2;

		poh.appendIndexEntry(30, 30, 7, len); len += 2;
		poh.appendIndexEntry(30, 30, 8, len); len += 2;
		poh.appendIndexEntry(30, 30, 9, len); len += 2;

		poh.appendIndexEntry(35, 35, 10, len); len += 2;
		poh.appendIndexEntry(45, 45, 11, len); len += 2;
		poh.appendIndexEntry(50, 50, 12, len); len += 2;

		poh.appendIndexEntry(55, 55, 13, len); len += 2;
		poh.appendIndexEntry(65, 65, 14, len); len += 2;
		poh.appendIndexEntry(75, 75, 15, len); len += 2;

		poh.appendIndexEntry(85, 85, 16, len); len += 2;
		poh.appendIndexEntry(93, 93, 17, len); len += 2;
		poh.appendIndexEntry(99, 99, 18, len); len += 2;

		poh.appendIndexEntry(105, 105, 19, len); len += 2;

		long[][] resType = new long[][]{{}};

		assertArrayEquals(new long[][]{{15, 14, 13}, {12, 11, 10}, {9, 8, 7}, {6, 5, 4}, {3, 2, 1}}, poh.findAllMinBased(3, 77,len, true).toArray(resType));
		assertArrayEquals(new long[][]{{6, 5, 4}}, poh.findAllMinBased(20, 25,len, true).toArray(resType));

		assertArrayEquals(new long[][]{{9, 8, 7}, {6, 5, 4}}, poh.findAllMinBased(20, 30,len, true).toArray(resType));

		assertNull(poh.findAllMinBased(1, 2,len, true)); //Out of lower limit
		assertArrayEquals(new long[][]{{19}}, poh.findAllMinBased(111, 112,len, true).toArray(resType)); //Out of upper limit
		assertArrayEquals(new long[][]{{13}}, poh.findAllMinBased(61, 63,len, true).toArray(resType)); //Out of range but b/w entries

		assertArrayEquals(new long[][]{{2, 1}}, poh.findAllMinBased(1, 8,len, true).toArray(resType)); // partially cover lower limit
		assertArrayEquals(new long[][]{{19}, {18, 17}}, poh.findAllMinBased(95, 111,len, true).toArray(resType)); // partially cover upper limit

		assertArrayEquals(new long[][]{{10}, {9, 8, 7}, {6, 5}}, poh.findAllMinBased(22, 42,len, true).toArray(resType)); // cover entries but fall across entries' ranges
		assertArrayEquals(new long[][]{{15, 14, 13}, {12, 11, 10}, {9}}, poh.findAllMinBased(32, 81,len, true).toArray(resType)); // cover entries but fall across entries' ranges and nodes
		assertArrayEquals(new long[][]{{15, 14, 13}, {12, 11}}, poh.findAllMinBased(47, 82,len, true).toArray(resType)); // cover entries from lower part
		assertArrayEquals(new long[][]{{17, 16}, {15}}, poh.findAllMinBased(82, 98,len, true).toArray(resType)); // cover entries from upper part

		assertArrayEquals(new long[][]{{9, 8, 7}, {6}}, poh.findAllMinBased(30, 30,len, true).toArray(resType)); // find all with same ts
	}

	@Test
	public void findAllTest() throws IOException, KawkabException, InterruptedException {
		System.out.println("Test: findAllTest");

		int epn = 3; // entries per node
		int cpn = 3; // children per node
		int nodeSize = epn*POHEntry.sizeBytes() + cpn*POHNode.childSizeBytes() + POHNode.headerSizeBytes();
		int nodesPerBlock = Configuration.instance().nodesPerBlockPOH;
		PostOrderHeapIndex poh = new PostOrderHeapIndex(12, nodeSize, nodesPerBlock, 50, new NullCache(), new NullTimerQueue());

		int len = 0;
		poh.appendMinTS(3, 1, len++); poh.appendMaxTS(5, 1, len++);
		poh.appendMinTS(8, 2, len++); poh.appendMaxTS(12, 2, len++);
		poh.appendMinTS(15, 3, len++); poh.appendMaxTS(18, 3, len++);

		poh.appendMinTS(18, 4, len++); poh.appendMaxTS(18, 4, len++);
		poh.appendMinTS(20, 5, len++); poh.appendMaxTS(23, 5, len++);
		poh.appendMinTS(25, 6, len++); poh.appendMaxTS(30, 6, len++);

		poh.appendMinTS(30, 7, len++); poh.appendMaxTS(30, 7, len++);
		poh.appendMinTS(30, 8, len++); poh.appendMaxTS(30, 8, len++);
		poh.appendMinTS(30, 9, len++); poh.appendMaxTS(30, 9, len++);

		poh.appendMinTS(35, 10, len++); poh.appendMaxTS(45, 10, len++);
		poh.appendMinTS(45, 11, len++); poh.appendMaxTS(48, 11, len++);
		poh.appendMinTS(50, 12, len++); poh.appendMaxTS(52, 12, len++);

		poh.appendMinTS(55, 13, len++); poh.appendMaxTS(55, 13, len++);
		poh.appendMinTS(65, 14, len++); poh.appendMaxTS(67, 14, len++);
		poh.appendMinTS(75, 15, len++); poh.appendMaxTS(77, 15, len++);

		poh.appendMinTS(85, 16, len++); poh.appendMaxTS(88, 16, len++);
		poh.appendMinTS(93, 17, len++); poh.appendMaxTS(95, 17, len++);
		poh.appendMinTS(99, 18, len++); poh.appendMaxTS(101, 18, len++);

		poh.appendMinTS(105, 19, len++);

		long[][] resType = new long[][]{{}};

		assertNull(poh.findAll(1, 2,len, true));
		assertNull(poh.findAll(102, 103,len, true));
		assertNull(poh.findAll(56, 58,len, true));
		assertArrayEquals(new long[][]{{19}}, poh.findAll(111, 112,len, true).toArray(resType));
	}

	@Test
	public void lastNodeIndexTest() {
		System.out.println("Test: lastNodeIndexTest");

		int epn = 3; // entries per node
		int cpn = 3; // children per node
		int nodeSize = epn*POHEntry.sizeBytes() + cpn*POHNode.childSizeBytes() + POHNode.headerSizeBytes();
		int nodesPerBlock = Configuration.instance().nodesPerBlockPOH;
		PostOrderHeapIndex poh = new PostOrderHeapIndex(13, nodeSize, nodesPerBlock, 50, new NullCache(), new NullTimerQueue());

		assertEquals(1, poh.lastNodeIndex(1, 3));
		assertEquals(1, poh.lastNodeIndex(6, 3));
		assertEquals(2, poh.lastNodeIndex(7, 3));
	}

	@Test
	public void blocksCreationTest() throws IOException, KawkabException {
		System.out.println("Test: blocksCreationTest");

		int epn = 1; // entries per node
		int cpn = 3; // children per node
		int nodeSize = epn*POHEntry.sizeBytes() + cpn*POHNode.childSizeBytes() + POHNode.headerSizeBytes();
		int nodesPerBlock = Configuration.instance().nodesPerBlockPOH;
		PostOrderHeapIndex poh = new PostOrderHeapIndex(14, nodeSize, nodesPerBlock, 25, new NullCache(), new NullTimerQueue());

		int count = nodesPerBlock * 5;
		int len = 0;
		for (int i=1; i<=count; i++) {
			poh.appendIndexEntry(i, i, i, len);
			len += 2;
		}
	}
}
