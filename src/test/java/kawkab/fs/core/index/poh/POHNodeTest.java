package kawkab.fs.core.index.poh;

import kawkab.fs.core.exceptions.IndexBlockFullException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class POHNodeTest {
	@Test
	public void searchSmokeTest() throws IndexBlockFullException {
		System.out.println("Test: entrySearchSmokeTest");

		POHNode node = new POHNode(1, 10, 4);

		node.appendPointer(1, 5, 1);
		node.appendPointer(5, 7, 2);
		node.appendPointer(5, 7, 3);
		node.appendPointer(5, 7, 4);


		Assertions.assertTrue(node.findFirstPointer(5) == 1);
		Assertions.assertTrue(node.findLastPointer(5) == 4);
		Assertions.assertArrayEquals(new int[]{1, 2, 3, 4}, node.findAllPointers(5));

		node.appendEntry(1, 2, 1);
		node.appendEntry(3, 4, 2);
		node.appendEntry(5, 6, 3);
		node.appendEntry(7, 8, 4);
		node.appendEntry(9, 10, 5);
		node.appendEntry(11, 12, 6);
		node.appendEntry(13, 14, 7);

		Assertions.assertTrue(node.findFirstEntry(5) == 3);
		Assertions.assertTrue(node.findLastEntry(9) == 5);
		Assertions.assertArrayEquals(new long[]{6}, node.findAllEntries(11));
	}

	@Test
	public void singleEntryTest() throws IndexBlockFullException {
		System.out.println("Test: singleEntryTest");

		POHNode node = new POHNode(1, 10, 2);

		node.appendEntry(1, 14, 7);

		Assertions.assertTrue(node.findFirstEntry(4) == 7);
		Assertions.assertTrue(node.findLastEntry(3) == 7);
		Assertions.assertArrayEquals(new long[]{7}, node.findAllEntries(2));
	}

	@Test
	public void noEntriesTest() throws IndexBlockFullException {
		System.out.println("Test: noEntriesTest");

		POHNode node = new POHNode(1, 10, 10);

		Assertions.assertTrue(node.findFirstPointer(5) < 0);
		Assertions.assertTrue(node.findLastPointer(5) < 0);
		Assertions.assertArrayEquals(null, node.findAllPointers(5));

		Assertions.assertTrue(node.findFirstEntry(4) < 0);
		Assertions.assertTrue(node.findLastEntry(4) < 0);
		Assertions.assertArrayEquals(null, node.findAllEntries(2));
	}

	@Test
	public void overlappingEntriesTest() throws IndexBlockFullException {
		System.out.println("Test: overlappingEntriesTest");

		POHNode node = new POHNode(1, 10, 8);

		node.appendPointer(1, 2, 1);
		node.appendPointer(2, 4, 2);
		node.appendPointer(4, 8, 3);
		node.appendPointer(8, 12, 4);
		node.appendPointer(8, 12, 5);
		node.appendPointer(8, 12, 6);
		node.appendPointer(15, 16, 7);
		node.appendPointer(15, 16, 8);

		Assertions.assertTrue(node.findFirstPointer(4) == 2);
		Assertions.assertTrue(node.findLastPointer(8) == 6);
		Assertions.assertArrayEquals(new int[]{3, 4, 5, 6}, node.findAllPointers(8));

		node.appendEntry(1, 2, 1);
		node.appendEntry(2, 4, 2);
		node.appendEntry(4, 8, 3);
		node.appendEntry(8, 12, 4);
		node.appendEntry(8, 12, 5);
		node.appendEntry(8, 12, 6);
		node.appendEntry(15, 16, 7);

		Assertions.assertTrue(node.findFirstEntry(4) == 2);
		Assertions.assertTrue(node.findLastEntry(8) == 6);
		Assertions.assertArrayEquals(new long[]{3, 4, 5, 6}, node.findAllEntries(8));
	}

	@Test
	public void notFoundTest() throws IndexBlockFullException {
		System.out.println("Test: notFoundTest");

		POHNode node = new POHNode(1, 10, 2);

		node.appendEntry(1, 2, 1);
		node.appendEntry(4, 5, 2);
		node.appendEntry(5, 6, 3);
		node.appendEntry(7, 8, 4);
		node.appendEntry(8, 9, 5);

		Assertions.assertTrue(node.findFirstEntry(3) < 0);
		Assertions.assertTrue(node.findLastEntry(3) < 0);
		Assertions.assertArrayEquals(null, node.findAllEntries(3));
	}

	@Test
	public void occurrenceTest() throws IndexBlockFullException {
		System.out.println("Test: occurrenceTest");

		POHNode node = new POHNode(1, 10, 2);

		node.appendEntry(1, 2, 1);
		node.appendEntry(5, 10, 2);
		node.appendEntry(5, 10, 3);
		node.appendEntry(5, 10, 4);
		node.appendEntry(5, 10, 5);
		node.appendEntry(11, 22, 6);
		node.appendEntry(32, 36, 7);
		node.appendEntry(36, 36, 8);

		Assertions.assertTrue(node.findFirstEntry(7) == 2);
		Assertions.assertTrue(node.findLastEntry(7) == 5);
		Assertions.assertArrayEquals(new long[]{2, 3, 4, 5}, node.findAllEntries(7));
	}

	@Test
	public void outOfRangeTest() throws IndexBlockFullException {
		System.out.println("Test: outOfRangeTest");

		POHNode node = new POHNode(1, 10, 2);

		node.appendEntry(5, 6, 1);
		node.appendEntry(7, 8, 2);
		node.appendEntry(8, 12, 3);
		node.appendEntry(17, 18, 4);
		node.appendEntry(27, 28, 4);

		Assertions.assertTrue(node.findFirstEntry(3) < 0);
		Assertions.assertTrue(node.findFirstEntry(30) < 0);
		Assertions.assertTrue(node.findLastEntry(3) < 0);
		Assertions.assertTrue(node.findLastEntry(30) < 0);
		Assertions.assertArrayEquals(null, node.findAllEntries(3));
		Assertions.assertArrayEquals(null, node.findAllEntries(30));
	}

	//TODO: Test for invalid arguments
	//TODO: Interleaving and concurrent append and search tests
}
