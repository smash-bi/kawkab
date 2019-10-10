package kawkab.fs.core.index.poh;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

public class PostOrderHeapIndexTest {
	@Test
	public void rootHeightFuncTest() throws IllegalAccessException, NoSuchMethodException, InvocationTargetException {
		System.out.println("Test: rootHeightFuncTest");
		PostOrderHeapIndex poh = new PostOrderHeapIndex(1, 3, 200);

		Assertions.assertEquals(0, heightOfRoot(poh, 3));
		Assertions.assertEquals(1, heightOfRoot(poh, 4));
		Assertions.assertEquals(2, heightOfRoot(poh, 13));
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
		Assertions.assertEquals(4, totalNodesKAryTree(poh, 1));
		Assertions.assertEquals(13, totalNodesKAryTree(poh, 2));
		Assertions.assertEquals(40, totalNodesKAryTree(poh, 3));

		poh = new PostOrderHeapIndex(1, 2, 200);
		Assertions.assertEquals(3, totalNodesKAryTree(poh, 1));
		Assertions.assertEquals(7, totalNodesKAryTree(poh, 2));
		Assertions.assertEquals(15, totalNodesKAryTree(poh, 3));

		poh = new PostOrderHeapIndex(1, 4, 200);
		Assertions.assertEquals(5, totalNodesKAryTree(poh, 1));
		Assertions.assertEquals(21, totalNodesKAryTree(poh, 2));
		Assertions.assertEquals(85, totalNodesKAryTree(poh, 3));
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
			poh.append(i, i);
		}

		Assertions.assertEquals(0, heightOfNode(poh, 1));
		Assertions.assertEquals(0, heightOfNode(poh, 3));
		Assertions.assertEquals(1, heightOfNode(poh, 4));
		Assertions.assertEquals(1, heightOfNode(poh, 21));
		Assertions.assertEquals(2, heightOfNode(poh, 26));
		Assertions.assertEquals(0, heightOfNode(poh, 29));
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
			poh.append(i, i);
		}
		Assertions.assertEquals(25, poh.findHighest(25));
		Assertions.assertEquals(19, poh.findHighest(19));
		Assertions.assertEquals(1, poh.findHighest(1));

		Assertions.assertEquals(29, poh.findHighest(30));
		Assertions.assertEquals(-1, poh.findHighest(0));
	}

	@Test
	public void searchOverlappingTest() {
		System.out.println("Test: searchOverlappingTest");

		PostOrderHeapIndex poh = new PostOrderHeapIndex(5, 3, 200);
		for (int i=1; i<=100; i++) {
			poh.append(i, i);
		}

		Assertions.assertEquals(2, poh.findHighest(2));
		Assertions.assertEquals(3, poh.findHighest(3));

		Assertions.assertEquals(100, poh.findHighest(102));
		Assertions.assertEquals(-1, poh.findHighest(0));
	}

	@Test
	public void findAllRangeTest() {
		System.out.println("Test: findAllRangeTest");

		PostOrderHeapIndex poh = new PostOrderHeapIndex(3, 3, 200);
		poh.append(3, 1);
		poh.append(5, 2);
		poh.append(11, 3);

		poh.append(18, 4);
		poh.append(20, 5);
		poh.append(25, 6);

		poh.append(30, 7);
		poh.append(30, 8);
		poh.append(30, 9);

		poh.append(35, 10);
		poh.append(45, 11);
		poh.append(50, 12);

		poh.append(55, 13);
		poh.append(65, 14);
		poh.append(75, 15);

		poh.append(85, 16);
		poh.append(93, 17);
		poh.append(99, 18);

		poh.append(105, 19);

		long[][] resType = new long[][]{{}};

		Assertions.assertArrayEquals(new long[][]{{15, 14, 13}, {12, 11, 10}, {9, 8, 7}, {6, 5, 4}, {3, 2, 1}}, poh.findAll(3, 77).toArray(resType));
		Assertions.assertArrayEquals(new long[][]{{6, 5, 4}}, poh.findAll(20, 25).toArray(resType));

		Assertions.assertArrayEquals(new long[][]{{9, 8, 7}, {6, 5, 4}}, poh.findAll(20, 30).toArray(resType));

		Assertions.assertEquals(null, poh.findAll(1, 2)); //Out of lower limit
		Assertions.assertArrayEquals(new long[][]{{19}}, poh.findAll(111, 112).toArray(resType)); //Out of upper limit
		Assertions.assertArrayEquals(new long[][]{{13}}, poh.findAll(61, 63).toArray(resType)); //Out of range but b/w entries

		Assertions.assertArrayEquals(new long[][]{{2, 1}}, poh.findAll(1, 8).toArray(resType)); // partially cover lower limit
		Assertions.assertArrayEquals(new long[][]{{19}, {18, 17}}, poh.findAll(95, 111).toArray(resType)); // partially cover upper limit

		Assertions.assertArrayEquals(new long[][]{{10}, {9, 8, 7}, {6, 5}}, poh.findAll(22, 42).toArray(resType)); // cover entries but fall across entries' ranges
		Assertions.assertArrayEquals(new long[][]{{15, 14, 13}, {12, 11, 10}, {9}}, poh.findAll(32, 81).toArray(resType)); // cover entries but fall across entries' ranges and nodes
		Assertions.assertArrayEquals(new long[][]{{15, 14, 13}, {12, 11}}, poh.findAll(47, 82).toArray(resType)); // cover entries from lower part
		Assertions.assertArrayEquals(new long[][]{{17, 16}, {15}}, poh.findAll(82, 98).toArray(resType)); // cover entries from upper part

		Assertions.assertArrayEquals(new long[][]{{9, 8, 7}, {6}}, poh.findAll(30, 30).toArray(resType)); // find all with same ts
	}

	@Test
	public void singleEntryTest() {
		System.out.println("Test: singleEntryTest");

		PostOrderHeapIndex poh = new PostOrderHeapIndex(3, 3, 200);
		poh.append(3, 1);

		long[][] resType = new long[][]{{}};

		Assertions.assertArrayEquals(new long[][]{{1}}, poh.findAll(1, 6).toArray(resType));
 		Assertions.assertArrayEquals(new long[][]{{1}}, poh.findAll(1, 3).toArray(resType));
		Assertions.assertArrayEquals(new long[][]{{1}}, poh.findAll(3, 3).toArray(resType));
		Assertions.assertArrayEquals(new long[][]{{1}}, poh.findAll(3, 5).toArray(resType));
		Assertions.assertEquals(null, poh.findAll(1, 2));
		Assertions.assertArrayEquals(new long[][]{{1}}, poh.findAll(6, 7).toArray(resType));
	}

	@Test
	public void exactMatchTest() {
		System.out.println("Test: exactMatchTest");

		PostOrderHeapIndex poh = new PostOrderHeapIndex(3, 3, 200);
		poh.append(3, 1);
		poh.append(5, 2);
		poh.append(11, 3);

		poh.append(18, 4);
		poh.append(20, 5);
		poh.append(25, 6);

		poh.append(30, 7);
		poh.append(30, 8);
		poh.append(30, 9);

		poh.append(35, 10);
		poh.append(45, 11);
		poh.append(50, 12);

		poh.append(55, 13);
		poh.append(65, 14);
		poh.append(75, 15);

		poh.append(85, 16);
		poh.append(93, 17);
		poh.append(99, 18);

		poh.append(105, 19);

		Assertions.assertEquals(-1, poh.findHighest(1)); //Out of lower limit

		Assertions.assertEquals(9, poh.findHighest(30));

		Assertions.assertEquals(12, poh.findHighest(50)); //Finding in an internal node
	}

	@Test
	public void sameMinMaxTest() {
		System.out.println("Test: sameMinMaxTest");

		PostOrderHeapIndex poh = new PostOrderHeapIndex(3, 3, 200);
		poh.append(3, 1);
		poh.append(5, 2);
		poh.append(15, 3);

		poh.append(18, 4);
		poh.append(20, 5);
		poh.append(25, 6);

		poh.append(30, 7);
		poh.append(30, 8);
		poh.append(30, 9);

		poh.append(35, 10);
		poh.append(45, 11);
		poh.append(50, 12);

		poh.append(55, 13);
		poh.append(65, 14);
		poh.append(75, 15);

		poh.append(85, 16);
		poh.append(93, 17);
		poh.append(99, 18);

		poh.append(105, 19);

		long[][] resType = new long[][]{{}};

		Assertions.assertArrayEquals(new long[][]{{15, 14, 13}, {12, 11, 10}, {9, 8, 7}, {6, 5, 4}, {3, 2, 1}}, poh.findAll(3, 77).toArray(resType));
		Assertions.assertArrayEquals(new long[][]{{6, 5, 4}}, poh.findAll(20, 25).toArray(resType));

		Assertions.assertArrayEquals(new long[][]{{9, 8, 7}, {6, 5, 4}}, poh.findAll(20, 30).toArray(resType));

		Assertions.assertEquals(null, poh.findAll(1, 2)); //Out of lower limit
		Assertions.assertArrayEquals(new long[][]{{19}}, poh.findAll(111, 112).toArray(resType)); //Out of upper limit
		Assertions.assertArrayEquals(new long[][]{{13}}, poh.findAll(61, 63).toArray(resType)); //Out of range but b/w entries

		Assertions.assertArrayEquals(new long[][]{{2, 1}}, poh.findAll(1, 8).toArray(resType)); // partially cover lower limit
		Assertions.assertArrayEquals(new long[][]{{19}, {18, 17}}, poh.findAll(95, 111).toArray(resType)); // partially cover upper limit

		Assertions.assertArrayEquals(new long[][]{{10}, {9, 8, 7}, {6, 5}}, poh.findAll(22, 42).toArray(resType)); // cover entries but fall across entries' ranges
		Assertions.assertArrayEquals(new long[][]{{15, 14, 13}, {12, 11, 10}, {9}}, poh.findAll(32, 81).toArray(resType)); // cover entries but fall across entries' ranges and nodes
		Assertions.assertArrayEquals(new long[][]{{15, 14, 13}, {12, 11}}, poh.findAll(47, 82).toArray(resType)); // cover entries from lower part
		Assertions.assertArrayEquals(new long[][]{{17, 16}, {15}}, poh.findAll(82, 98).toArray(resType)); // cover entries from upper part

		Assertions.assertArrayEquals(new long[][]{{9, 8, 7}, {6}}, poh.findAll(30, 30).toArray(resType)); // find all with same ts
	}
}
