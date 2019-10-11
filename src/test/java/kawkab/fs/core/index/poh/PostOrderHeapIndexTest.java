package kawkab.fs.core.index.poh;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.*;

public class PostOrderHeapIndexTest {
	@Test
	public void rootHeightFuncTest() throws IllegalAccessException, NoSuchMethodException, InvocationTargetException {
		System.out.println("Test: rootHeightFuncTest");
		PostOrderHeapIndex poh = new PostOrderHeapIndex(1, 3, 200);

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
		PostOrderHeapIndex poh = new PostOrderHeapIndex(1, 3, 200);
		assertEquals(4, totalNodesKAryTree(poh, 1));
		assertEquals(13, totalNodesKAryTree(poh, 2));
		assertEquals(40, totalNodesKAryTree(poh, 3));

		poh = new PostOrderHeapIndex(1, 2, 200);
		assertEquals(3, totalNodesKAryTree(poh, 1));
		assertEquals(7, totalNodesKAryTree(poh, 2));
		assertEquals(15, totalNodesKAryTree(poh, 3));

		poh = new PostOrderHeapIndex(1, 4, 200);
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
	public void pohStructureTest() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
		System.out.println("Test: pohStructureTest");
		PostOrderHeapIndex poh = new PostOrderHeapIndex(1, 3, 200);

		for (int i=1; i<30; i++) {
			poh.appendIndexEntry(i, i, i);
		}

		assertEquals(0, heightOfNode(poh, 1));
		assertEquals(0, heightOfNode(poh, 3));
		assertEquals(1, heightOfNode(poh, 4));
		assertEquals(1, heightOfNode(poh, 21));
		assertEquals(2, heightOfNode(poh, 26));
		assertEquals(0, heightOfNode(poh, 29));
	}

	private int heightOfNode(PostOrderHeapIndex poh, int nodeNumber) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
		Method method = PostOrderHeapIndex.class.getDeclaredMethod("heightOfNode", int.class);
		method.setAccessible(true);
		return (int) method.invoke(poh, nodeNumber);
	}

	@Test
	public void searchSmokeTest() {
		System.out.println("Test: searchFirstTest");

		PostOrderHeapIndex poh = new PostOrderHeapIndex(1, 3, 200);
		for (int i=1; i<30; i++) {
			poh.appendIndexEntry(i, i, i);
		}
		assertEquals(25, poh.findHighest(25));
		assertEquals(19, poh.findHighest(19));
		assertEquals(1, poh.findHighest(1));

		assertEquals(29, poh.findHighest(30));
		assertEquals(-1, poh.findHighest(0));
	}

	@Test
	public void searchOverlappingTest() {
		System.out.println("Test: searchOverlappingTest");

		PostOrderHeapIndex poh = new PostOrderHeapIndex(5, 3, 200);
		for (int i=1; i<=100; i++) {
			poh.appendIndexEntry(i, i, i);
		}

		assertEquals(2, poh.findHighest(2));
		assertEquals(3, poh.findHighest(3));

		assertEquals(100, poh.findHighest(102));
		assertEquals(-1, poh.findHighest(0));
	}

	@Test
	public void findAllRangeTest() {
		System.out.println("Test: findAllRangeTest");

		PostOrderHeapIndex poh = new PostOrderHeapIndex(3, 3, 200);
		poh.appendMinTS(3, 1);
		poh.appendMaxTS(3, 1);
		poh.appendMinTS(5, 2);
		poh.appendMaxTS(5, 2);
		poh.appendMinTS(11, 3);
		poh.appendMaxTS(11, 3);

		poh.appendMinTS(18, 4);
		poh.appendMaxTS(18, 4);
		poh.appendMinTS(20, 5);
		poh.appendMaxTS(20, 5);
		poh.appendMinTS(25, 6);
		poh.appendMaxTS(25, 6);

		poh.appendMinTS(30, 7);
		poh.appendMaxTS(30, 7);
		poh.appendMinTS(30, 8);
		poh.appendMaxTS(30, 8);
		poh.appendMinTS(30, 9);
		poh.appendMaxTS(30, 9);

		poh.appendMinTS(35, 10);
		poh.appendMaxTS(35, 10);
		poh.appendMinTS(45, 11);
		poh.appendMaxTS(45, 11);
		poh.appendMinTS(50, 12);
		poh.appendMaxTS(50, 12);

		poh.appendMinTS(55, 13);
		poh.appendMaxTS(55, 13);
		poh.appendMinTS(65, 14);
		poh.appendMaxTS(65, 14);
		poh.appendMinTS(75, 15);
		poh.appendMaxTS(75, 15);

		poh.appendMinTS(85, 16);
		poh.appendMaxTS(85, 16);
		poh.appendMinTS(93, 17);
		poh.appendMaxTS(93, 17);
		poh.appendMinTS(99, 18);
		poh.appendMaxTS(99, 18);

		poh.appendMinTS(105, 19);
		poh.appendMaxTS(105, 19);

		long[][] resType = new long[][]{{}};

		assertArrayEquals(new long[][]{{15, 14, 13}, {12, 11, 10}, {9, 8, 7}, {6, 5, 4}, {3, 2, 1}}, poh.findAllMinBased(3, 77).toArray(resType));
		assertArrayEquals(new long[][]{{6, 5, 4}}, poh.findAllMinBased(20, 25).toArray(resType));

		assertArrayEquals(new long[][]{{9, 8, 7}, {6, 5, 4}}, poh.findAllMinBased(20, 30).toArray(resType));

		assertEquals(null, poh.findAllMinBased(1, 2)); //Out of lower limit
		assertArrayEquals(new long[][]{{19}}, poh.findAllMinBased(111, 112).toArray(resType)); //Out of upper limit
		assertArrayEquals(new long[][]{{13}}, poh.findAllMinBased(61, 63).toArray(resType)); //Out of range but b/w entries

		assertArrayEquals(new long[][]{{2, 1}}, poh.findAllMinBased(1, 8).toArray(resType)); // partially cover lower limit
		assertArrayEquals(new long[][]{{19}, {18, 17}}, poh.findAllMinBased(95, 111).toArray(resType)); // partially cover upper limit

		assertArrayEquals(new long[][]{{10}, {9, 8, 7}, {6, 5}}, poh.findAllMinBased(22, 42).toArray(resType)); // cover entries but fall across entries' ranges
		assertArrayEquals(new long[][]{{15, 14, 13}, {12, 11, 10}, {9}}, poh.findAllMinBased(32, 81).toArray(resType)); // cover entries but fall across entries' ranges and nodes
		assertArrayEquals(new long[][]{{15, 14, 13}, {12, 11}}, poh.findAllMinBased(47, 82).toArray(resType)); // cover entries from lower part
		assertArrayEquals(new long[][]{{17, 16}, {15}}, poh.findAllMinBased(82, 98).toArray(resType)); // cover entries from upper part

		assertArrayEquals(new long[][]{{9, 8, 7}, {6}}, poh.findAllMinBased(30, 30).toArray(resType)); // find all with same ts
	}

	@Test
	public void singleEntryTest() {
		System.out.println("Test: singleEntryTest");

		PostOrderHeapIndex poh = new PostOrderHeapIndex(3, 3, 200);
		poh.appendMinTS(3, 1);

		long[][] resType = new long[][]{{}};

		assertArrayEquals(new long[][]{{1}}, poh.findAllMinBased(1, 6).toArray(resType));
 		assertArrayEquals(new long[][]{{1}}, poh.findAllMinBased(1, 3).toArray(resType));
		assertArrayEquals(new long[][]{{1}}, poh.findAllMinBased(3, 3).toArray(resType));
		assertArrayEquals(new long[][]{{1}}, poh.findAllMinBased(3, 5).toArray(resType));
		assertNull(poh.findAllMinBased(1, 2));
		assertArrayEquals(new long[][]{{1}}, poh.findAllMinBased(6, 7).toArray(resType));
	}

	@Test
	public void exactMatchTest() {
		System.out.println("Test: exactMatchTest");

		PostOrderHeapIndex poh = new PostOrderHeapIndex(3, 3, 200);
		poh.appendMinTS(3, 1);
		poh.appendMaxTS(3, 1);
		poh.appendMinTS(5, 2);
		poh.appendMaxTS(5, 2);
		poh.appendMinTS(11, 3);
		poh.appendMaxTS(11, 3);

		poh.appendMinTS(18, 4);
		poh.appendMaxTS(18, 4);
		poh.appendMinTS(20, 5);
		poh.appendMaxTS(20, 5);
		poh.appendMinTS(25, 6);
		poh.appendMaxTS(25, 6);

		poh.appendMinTS(30, 7);
		poh.appendMaxTS(30, 7);
		poh.appendMinTS(30, 8);
		poh.appendMaxTS(30, 8);
		poh.appendMinTS(30, 9);
		poh.appendMaxTS(30, 9);

		poh.appendMinTS(35, 10);
		poh.appendMaxTS(35, 10);
		poh.appendMinTS(45, 11);
		poh.appendMaxTS(45, 11);
		poh.appendMinTS(50, 12);
		poh.appendMaxTS(50, 12);

		poh.appendMinTS(55, 13);
		poh.appendMaxTS(55, 13);
		poh.appendMinTS(65, 14);
		poh.appendMaxTS(65, 14);
		poh.appendMinTS(75, 15);
		poh.appendMaxTS(75, 15);

		poh.appendMinTS(85, 16);
		poh.appendMaxTS(85, 16);
		poh.appendMinTS(93, 17);
		poh.appendMaxTS(93, 17);
		poh.appendMinTS(99, 18);
		poh.appendMaxTS(99, 18);

		poh.appendMinTS(105, 19);
		poh.appendMaxTS(105, 19);

		assertEquals(-1, poh.findHighest(1)); //Out of lower limit

		assertEquals(9, poh.findHighest(30));

		assertEquals(12, poh.findHighest(50)); //Finding in an internal node
	}

	@Test
	public void sameMinMaxTest() {
		System.out.println("Test: sameMinMaxTest");

		PostOrderHeapIndex poh = new PostOrderHeapIndex(3, 3, 200);
		poh.appendMinTS(3, 1);
		poh.appendMaxTS(3, 1);
		poh.appendMinTS(5, 2);
		poh.appendMaxTS(5, 2);
		poh.appendMinTS(11, 3);
		poh.appendMaxTS(11, 3);

		poh.appendMinTS(18, 4);
		poh.appendMaxTS(18, 4);
		poh.appendMinTS(20, 5);
		poh.appendMaxTS(20, 5);
		poh.appendMinTS(25, 6);
		poh.appendMaxTS(25, 6);

		poh.appendMinTS(30, 7);
		poh.appendMaxTS(30, 7);
		poh.appendMinTS(30, 8);
		poh.appendMaxTS(30, 8);
		poh.appendMinTS(30, 9);
		poh.appendMaxTS(30, 9);

		poh.appendMinTS(35, 10);
		poh.appendMaxTS(35, 10);
		poh.appendMinTS(45, 11);
		poh.appendMaxTS(45, 11);
		poh.appendMinTS(50, 12);
		poh.appendMaxTS(50, 12);

		poh.appendMinTS(55, 13);
		poh.appendMaxTS(55, 13);
		poh.appendMinTS(65, 14);
		poh.appendMaxTS(65, 14);
		poh.appendMinTS(75, 15);
		poh.appendMaxTS(75, 15);

		poh.appendMinTS(85, 16);
		poh.appendMaxTS(85, 16);
		poh.appendMinTS(93, 17);
		poh.appendMaxTS(93, 17);
		poh.appendMinTS(99, 18);
		poh.appendMaxTS(99, 18);

		poh.appendMinTS(105, 19);
		poh.appendMaxTS(105, 19);

		long[][] resType = new long[][]{{}};

		assertArrayEquals(new long[][]{{15, 14, 13}, {12, 11, 10}, {9, 8, 7}, {6, 5, 4}, {3, 2, 1}}, poh.findAllMinBased(3, 77).toArray(resType));
		assertArrayEquals(new long[][]{{6, 5, 4}}, poh.findAllMinBased(20, 25).toArray(resType));

		assertArrayEquals(new long[][]{{9, 8, 7}, {6, 5, 4}}, poh.findAllMinBased(20, 30).toArray(resType));

		assertNull(poh.findAllMinBased(1, 2)); //Out of lower limit
		assertArrayEquals(new long[][]{{19}}, poh.findAllMinBased(111, 112).toArray(resType)); //Out of upper limit
		assertArrayEquals(new long[][]{{13}}, poh.findAllMinBased(61, 63).toArray(resType)); //Out of range but b/w entries

		assertArrayEquals(new long[][]{{2, 1}}, poh.findAllMinBased(1, 8).toArray(resType)); // partially cover lower limit
		assertArrayEquals(new long[][]{{19}, {18, 17}}, poh.findAllMinBased(95, 111).toArray(resType)); // partially cover upper limit

		assertArrayEquals(new long[][]{{10}, {9, 8, 7}, {6, 5}}, poh.findAllMinBased(22, 42).toArray(resType)); // cover entries but fall across entries' ranges
		assertArrayEquals(new long[][]{{15, 14, 13}, {12, 11, 10}, {9}}, poh.findAllMinBased(32, 81).toArray(resType)); // cover entries but fall across entries' ranges and nodes
		assertArrayEquals(new long[][]{{15, 14, 13}, {12, 11}}, poh.findAllMinBased(47, 82).toArray(resType)); // cover entries from lower part
		assertArrayEquals(new long[][]{{17, 16}, {15}}, poh.findAllMinBased(82, 98).toArray(resType)); // cover entries from upper part

		assertArrayEquals(new long[][]{{9, 8, 7}, {6}}, poh.findAllMinBased(30, 30).toArray(resType)); // find all with same ts
	}

	@Test
	public void findAllTest() {
		System.out.println("Test: findAllTest");

		PostOrderHeapIndex poh = new PostOrderHeapIndex(3, 3, 200);
		poh.appendMinTS(3, 1); poh.appendMaxTS(5, 1);
		poh.appendMinTS(8, 2); poh.appendMaxTS(12, 2);
		poh.appendMinTS(15, 3); poh.appendMaxTS(18, 3);

		poh.appendMinTS(18, 4); poh.appendMaxTS(18, 4);
		poh.appendMinTS(20, 5); poh.appendMaxTS(23, 5);
		poh.appendMinTS(25, 6); poh.appendMaxTS(30, 6);

		poh.appendMinTS(30, 7); poh.appendMaxTS(30, 7);
		poh.appendMinTS(30, 8); poh.appendMaxTS(30, 8);
		poh.appendMinTS(30, 9); poh.appendMaxTS(30, 9);

		poh.appendMinTS(35, 10); poh.appendMaxTS(45, 10);
		poh.appendMinTS(45, 11); poh.appendMaxTS(48, 11);
		poh.appendMinTS(50, 12); poh.appendMaxTS(52, 12);

		poh.appendMinTS(55, 13); poh.appendMaxTS(55, 13);
		poh.appendMinTS(65, 14); poh.appendMaxTS(67, 14);
		poh.appendMinTS(75, 15); poh.appendMaxTS(77, 15);

		poh.appendMinTS(85, 16); poh.appendMaxTS(88, 16);
		poh.appendMinTS(93, 17); poh.appendMaxTS(95, 17);
		poh.appendMinTS(99, 18); poh.appendMaxTS(101, 18);

		poh.appendMinTS(105, 19);

		long[][] resType = new long[][]{{}};

		assertNull(poh.findAll(1, 2));
		assertNull(poh.findAll(102, 103));
		assertNull(poh.findAll(56, 58));
		assertArrayEquals(new long[][]{{19}}, poh.findAll(111, 112).toArray(resType));
	}

	@Test
	public void lastNodeIndexTest() {
		System.out.println("Test: lastNodeIndexTest");

		PostOrderHeapIndex poh = new PostOrderHeapIndex(3, 3, 200);

		assertEquals(1, poh.lastNodeIndex(1, 3));
		assertEquals(1, poh.lastNodeIndex(6, 3));
		assertEquals(2, poh.lastNodeIndex(7, 3));
	}
}
