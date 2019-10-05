package kawkab.fs.core.index.poh;

import kawkab.fs.core.exceptions.IndexBlockFullException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;

public class PostOrderHeapIndexTest {
	@Test
	public void rootHeightFuncTest() throws IllegalAccessException, NoSuchMethodException, InvocationTargetException {
		System.out.println("Test: rootHeightFuncTest");
		PostOrderHeapIndex poh = new PostOrderHeapIndex(1, 3);

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
		PostOrderHeapIndex poh = new PostOrderHeapIndex(1, 3);
		Assertions.assertEquals(4, totalNodesKAryTree(poh, 1));
		Assertions.assertEquals(13, totalNodesKAryTree(poh, 2));
		Assertions.assertEquals(40, totalNodesKAryTree(poh, 3));

		poh = new PostOrderHeapIndex(1, 2);
		Assertions.assertEquals(3, totalNodesKAryTree(poh, 1));
		Assertions.assertEquals(7, totalNodesKAryTree(poh, 2));
		Assertions.assertEquals(15, totalNodesKAryTree(poh, 3));

		poh = new PostOrderHeapIndex(1, 4);
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
		PostOrderHeapIndex poh = new PostOrderHeapIndex(1, 3);

		for (int i=1; i<30; i++) {
			poh.insert(i, i, i);
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

		PostOrderHeapIndex poh = new PostOrderHeapIndex(1, 3);
		for (int i=1; i<30; i++) {
			poh.insert(i, i, i);
		}
		Assertions.assertEquals(25, poh.find(25, false));
		Assertions.assertEquals(19, poh.find(19, false));
		Assertions.assertEquals(1, poh.find(1, false));

		Assertions.assertEquals(25, poh.find(25, true));
		Assertions.assertEquals(19, poh.find(19, true));
		Assertions.assertEquals(1, poh.find(1, true));

		Assertions.assertEquals(-1, poh.find(30, false));
		Assertions.assertEquals(-1, poh.find(0, false));
		Assertions.assertEquals(-1, poh.find(30, true));
		Assertions.assertEquals(-1, poh.find(0, true));
	}

	@Test
	public void searchOverlappingTest() {
		System.out.println("Test: searchOverlappingTest");

		PostOrderHeapIndex poh = new PostOrderHeapIndex(5, 3);
		for (int i=1; i<=100; i++) {
			poh.insert(i, i+1, i);
		}

		Assertions.assertEquals(1, poh.find(1, false));
		Assertions.assertEquals(1, poh.find(2, false));

		Assertions.assertEquals(2, poh.find(2, true));
		Assertions.assertEquals(3, poh.find(3, true));

		Assertions.assertEquals(-1, poh.find(102, false));
		Assertions.assertEquals(-1, poh.find(0, false));

		Assertions.assertEquals(-1, poh.find(102, true));
		Assertions.assertEquals(-1, poh.find(0, true));
	}

	@Test
	public void findAllRangeTest() {
		System.out.println("Test: findAllRangeTest");

		PostOrderHeapIndex poh = new PostOrderHeapIndex(3, 3);
		poh.insert(3, 5, 1);
		poh.insert(5, 10, 2);
		poh.insert(11, 15, 3);

		poh.insert(18, 20, 4);
		poh.insert(20, 20, 5);
		poh.insert(25, 30, 6);

		poh.insert(30, 30, 7);
		poh.insert(30, 30, 8);
		poh.insert(30, 30, 9);

		poh.insert(35, 40, 10);
		poh.insert(45, 50, 11);
		poh.insert(50, 55, 12);

		poh.insert(55, 60, 13);
		poh.insert(65, 70, 14);
		poh.insert(75, 80, 15);

		poh.insert(85, 90, 16);
		poh.insert(93, 96, 17);
		poh.insert(99, 102, 18);

		poh.insert(105, 110, 19);

		Assertions.assertArrayEquals(new long[][]{{15, 14, 13}, {12, 11, 10}, {9, 8, 7}, {6, 5, 4}, {3, 2, 1}}, poh.findAll(3, 77));
		Assertions.assertArrayEquals(new long[][]{{6, 5, 4}}, poh.findAll(20, 25));

		Assertions.assertArrayEquals(new long[][]{{9, 8, 7}, {6, 5, 4}}, poh.findAll(20, 30));

		Assertions.assertArrayEquals(null, poh.findAll(1, 2)); //Out of lower limit
		Assertions.assertArrayEquals(null, poh.findAll(111, 112)); //Out of upper limit
		Assertions.assertArrayEquals(null, poh.findAll(61, 63)); //Out of range but b/w entries

		Assertions.assertArrayEquals(new long[][]{{2, 1}}, poh.findAll(1, 8)); // partially cover lower limit
		Assertions.assertArrayEquals(new long[][]{{19}, {18}}, poh.findAll(100, 111)); // partially cover upper limit

		Assertions.assertArrayEquals(new long[][]{{10}, {9, 8, 7}, {6}}, poh.findAll(22, 42)); // cover entries but fall across entries' ranges
		Assertions.assertArrayEquals(new long[][]{{15, 14, 13}, {12, 11, 10}}, poh.findAll(32, 81)); // cover entries but fall across entries' ranges and nodes
		Assertions.assertArrayEquals(new long[][]{{15, 14, 13}, {12, 11}}, poh.findAll(47, 82)); // cover entries from lower part
		Assertions.assertArrayEquals(new long[][]{{17, 16}}, poh.findAll(82, 98)); // cover entries from upper part

		Assertions.assertArrayEquals(new long[][]{{9, 8, 7}, {6}}, poh.findAll(30, 30)); // find all with same ts
	}

	@Test
	public void singleEntryTest() {
		System.out.println("Test: singleEntryTest");

		PostOrderHeapIndex poh = new PostOrderHeapIndex(3, 3);
		poh.insert(3, 5, 1);

		Assertions.assertArrayEquals(new long[][]{{1}}, poh.findAll(1, 6));
		Assertions.assertArrayEquals(new long[][]{{1}}, poh.findAll(1, 4));
		Assertions.assertArrayEquals(new long[][]{{1}}, poh.findAll(4, 6));
		Assertions.assertArrayEquals(null, poh.findAll(1, 2));
		Assertions.assertArrayEquals(null, poh.findAll(6, 7));
	}

	@Test
	public void exactMatchTest() {
		System.out.println("Test: exactMatchTest");

		PostOrderHeapIndex poh = new PostOrderHeapIndex(3, 3);
		poh.insert(3, 5, 1);
		poh.insert(5, 10, 2);
		poh.insert(11, 15, 3);

		poh.insert(18, 20, 4);
		poh.insert(20, 20, 5);
		poh.insert(25, 30, 6);

		poh.insert(30, 30, 7);
		poh.insert(30, 30, 8);
		poh.insert(30, 30, 9);

		poh.insert(35, 40, 10);
		poh.insert(45, 50, 11);
		poh.insert(50, 55, 12);

		poh.insert(55, 60, 13);
		poh.insert(65, 70, 14);
		poh.insert(75, 80, 15);

		poh.insert(85, 90, 16);
		poh.insert(93, 96, 17);
		poh.insert(99, 102, 18);

		poh.insert(105, 110, 19);

		Assertions.assertEquals(-1, poh.find(1, false)); //Out of lower limit
		Assertions.assertEquals(-1, poh.find(1, true)); //Out of lower limit

		Assertions.assertEquals(6, poh.find(30, false));
		Assertions.assertEquals(9, poh.find(30, true));

		Assertions.assertEquals(11, poh.find(50, false)); //Finding in an internal node
		Assertions.assertEquals(12, poh.find(50, true)); //Finding in an internal node
	}
}
