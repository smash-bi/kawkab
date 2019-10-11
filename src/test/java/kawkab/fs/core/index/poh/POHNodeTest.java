package kawkab.fs.core.index.poh;

import kawkab.fs.core.exceptions.IndexBlockFullException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class POHNodeTest {
	@Test
	public void searchSmokeTest() throws IndexBlockFullException {
		System.out.println("Test: entrySearchSmokeTest");

		POHNode node = new POHNode(1, 0, 10, 4);

		node.appendEntryMinTS(1, 1);
		node.appendEntryMinTS(3, 2);
		node.appendEntryMinTS(5, 3);
		node.appendEntryMinTS(7, 4);
		node.appendEntryMinTS(9, 5);
		node.appendEntryMinTS(11, 6);
		node.appendEntryMinTS(13, 7);

		Assertions.assertEquals(2, node.findFirstEntry(5));
		Assertions.assertEquals(5, node.findLastEntry(9));
		Assertions.assertArrayEquals(new long[]{6, 5}, node.findAllEntriesMinBased(11, 11));
	}

	@Test
	public void singleEntryTest() throws IndexBlockFullException {
		System.out.println("Test: singleEntryTest");

		POHNode node = new POHNode(1, 0, 10, 2);

		node.appendEntryMinTS(5, 7);

		Assertions.assertEquals(7, node.findFirstEntry(5));
		Assertions.assertEquals(7, node.findLastEntry(5));
		Assertions.assertArrayEquals(new long[]{7}, node.findAllEntriesMinBased(6, 6));
	}

	@Test
	public void noEntriesTest() {
		System.out.println("Test: noEntriesTest");

		POHNode node = new POHNode(1, 0, 10, 10);

		Assertions.assertTrue(node.findFirstEntry(4) < 0);
		Assertions.assertTrue(node.findLastEntry(4) < 0);
		Assertions.assertArrayEquals(null, node.findAllEntriesMinBased(2, 2));
	}

	@Test
	public void overlappingEntriesTest() throws IndexBlockFullException {
		System.out.println("Test: overlappingEntriesTest");

		POHNode node = new POHNode(1, 0, 10, 8);

		node.appendEntryMinTS(1, 1);
		node.appendEntryMinTS(2, 2);
		node.appendEntryMinTS(4, 3);
		node.appendEntryMinTS(12, 4);
		node.appendEntryMinTS(12, 5);
		node.appendEntryMinTS(12, 6);
		node.appendEntryMinTS(15, 7);

		Assertions.assertEquals(3, node.findFirstEntry(12));
		Assertions.assertEquals(6, node.findLastEntry(12));
		Assertions.assertArrayEquals(new long[]{6, 5, 4, 3}, node.findAllEntriesMinBased(12, 12));
	}

	@Test
	public void notFoundTest() throws IndexBlockFullException {
		System.out.println("Test: notFoundTest");

		POHNode node = new POHNode(1, 0, 10, 2);

		node.appendEntryMinTS(2, 1);
		node.appendEntryMinTS(4, 2);
		node.appendEntryMinTS(5, 3);
		node.appendEntryMinTS(7, 4);
		node.appendEntryMinTS(8, 5);

		Assertions.assertTrue(node.findFirstEntry(1) < 0);
		Assertions.assertTrue(node.findLastEntry(1) < 0);
		Assertions.assertArrayEquals(null, node.findAllEntriesMinBased(1, 1));

		Assertions.assertTrue(node.findFirstEntry(3) >= 0);
		Assertions.assertTrue(node.findFirstEntry(10) >= 0);
	}

	@Test
	public void occurrenceTest() throws IndexBlockFullException {
		System.out.println("Test: occurrenceTest");

		POHNode node = new POHNode(1, 0, 10, 2);

		node.appendEntryMinTS(1, 1);
		node.appendEntryMinTS(5, 2);
		node.appendEntryMinTS(10, 3);
		node.appendEntryMinTS(10, 4);
		node.appendEntryMinTS(10, 5);
		node.appendEntryMinTS(11, 6);
		node.appendEntryMinTS(32, 7);
		node.appendEntryMinTS(36, 8);

		Assertions.assertEquals(2, node.findFirstEntry(7));
		Assertions.assertEquals(2, node.findFirstEntry(10));
		Assertions.assertEquals(5, node.findLastEntry(10));
		Assertions.assertArrayEquals(new long[]{5, 4, 3, 2}, node.findAllEntriesMinBased(10, 10));
	}

	@Test
	public void outOfRangeTest() throws IndexBlockFullException {
		System.out.println("Test: outOfRangeTest");

		POHNode node = new POHNode(1, 0, 10, 2);

		node.appendEntryMinTS(5, 1);
		node.appendEntryMinTS(7, 2);
		node.appendEntryMinTS(8, 3);
		node.appendEntryMinTS(17, 4);
		node.appendEntryMinTS(27, 5);

		Assertions.assertTrue(node.findFirstEntry(3) < 0);
		Assertions.assertTrue(node.findFirstEntry(30) >= 0);
		Assertions.assertTrue(node.findLastEntry(3) < 0);
		Assertions.assertTrue(node.findLastEntry(30) >= 0);
		Assertions.assertArrayEquals(null, node.findAllEntriesMinBased(3, 3));
		Assertions.assertArrayEquals(new long[]{5}, node.findAllEntriesMinBased(30, 30));
	}

	@Test
	public void rangeSearchTest() throws IndexBlockFullException {
		System.out.println("Test: rangeSearchTest");

		POHNode node = new POHNode(1, 0, 10, 2);

		node.appendEntryMinTS(5, 1);
		node.appendEntryMinTS(8, 2);
		node.appendEntryMinTS(15, 3);
		node.appendEntryMinTS(20, 4);
		node.appendEntryMinTS(25, 5);
		node.appendEntryMinTS(33, 6);
		node.appendEntryMinTS(40, 7);

		Assertions.assertArrayEquals(new long[]{4, 3, 2}, node.findAllEntriesMinBased(12, 20)); //Lower limit out of range
		Assertions.assertArrayEquals(new long[]{5, 4, 3}, node.findAllEntriesMinBased(20, 32)); //Upper limit out of range
		Assertions.assertArrayEquals(new long[]{5, 4, 3, 2}, node.findAllEntriesMinBased(12, 32)); //Both limits out of range but have some entries within the limits
		Assertions.assertArrayEquals(new long[]{7, 6, 5, 4, 3, 2, 1}, node.findAllEntriesMinBased(4, 51)); //all entries within the limits but the limits are not exact
		Assertions.assertArrayEquals(new long[]{2}, node.findAllEntriesMinBased(12, 13)); //no match
		Assertions.assertArrayEquals(new long[]{3, 2}, node.findAllEntriesMinBased(12, 15)); //only ceil match
		Assertions.assertArrayEquals(new long[]{2, 1}, node.findAllEntriesMinBased(8, 12)); //only floor match
		Assertions.assertArrayEquals(null, node.findAllEntriesMinBased(1, 4)); //range lower than min
		Assertions.assertArrayEquals(new long[]{7}, node.findAllEntriesMinBased(51, 55)); //range higher than max
		Assertions.assertArrayEquals(new long[]{2, 1}, node.findAllEntriesMinBased(4, 12)); //upper limit have entries
		Assertions.assertArrayEquals(new long[]{7, 6}, node.findAllEntriesMinBased(37, 55)); //lower limit have entries
		Assertions.assertArrayEquals(new long[]{4, 3, 2}, node.findAllEntriesMinBased(15, 20)); //Exact range test
	}

	@Test
	public void findAllEntriesTest() throws IndexBlockFullException {
		System.out.println("Test: findAllEntriesTest");

		POHNode node = new POHNode(1, 0, 10, 2);

		node.appendEntry(5, 7, 1);
		node.appendEntry(9, 12, 2);
		node.appendEntry(15, 17, 3);
		node.appendEntry(20, 20, 4);
		node.appendEntry(20, 20, 5);
		node.appendEntry(20, 25, 6);
		node.appendEntry(30, 40, 7);

		Assertions.assertEquals(null, node.findAllEntries(1, 3));
		Assertions.assertEquals(null, node.findAllEntries(41, 44));
		Assertions.assertEquals(null, node.findAllEntries(13, 14));

		Assertions.assertArrayEquals(new long[]{1}, node.findAllEntries(1, 8));
		Assertions.assertArrayEquals(new long[]{7}, node.findAllEntries(35, 45));
		Assertions.assertArrayEquals(new long[]{6, 5, 4, 3}, node.findAllEntries(17, 20));
		Assertions.assertArrayEquals(new long[]{6, 5, 4}, node.findAllEntries(20, 20));
	}

	@Test
	public void findAllEntriesHalfOpenTest() throws IndexBlockFullException {
		System.out.println("Test: findAllEntriesTest");

		POHNode node = new POHNode(1, 0, 10, 2);

		node.appendEntry(20, 20, 5);
		node.appendEntry(20, 25, 6);
		node.appendEntryMinTS(30, 7);

		Assertions.assertArrayEquals(null, node.findAllEntries(35, 45));
		Assertions.assertArrayEquals(new long[]{7}, node.findAllEntries(27, 35));
	}

	//TODO: Test for invalid arguments
	//TODO: Interleaving and concurrent append and search tests
}
