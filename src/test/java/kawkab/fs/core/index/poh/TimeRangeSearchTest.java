package kawkab.fs.core.index.poh;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TimeRangeSearchTest {
	@Test
	public void findLastFloorTest() {
		int count = 9;
		TimeRange[] t = new TimeRange[count];
		int offset = 10;

		for(int i=0; i<count; i++) {
			t[i] = new POHEntry(i*offset+offset, i+1);
		}

		Assertions.assertEquals(0, TimeRangeSearch.findLastFloor(t, count, 17)); // Have a value
		Assertions.assertEquals(-1, TimeRangeSearch.findLastFloor(t, count, 9)); //ts lower than the first value
		Assertions.assertEquals(8, TimeRangeSearch.findLastFloor(t, count, 120)); //ts larger than the first value
	}

	@Test
	public void findLastFloorSingleEntryTest() {
		TimeRange[] t = new TimeRange[]{new POHEntry(10, 1)};

		Assertions.assertEquals(0, TimeRangeSearch.findLastFloor(t, 1, 17)); // Have a value
		Assertions.assertEquals(-1, TimeRangeSearch.findLastFloor(t, 1, 9)); //ts lower than the first value
		Assertions.assertEquals(0, TimeRangeSearch.findLastFloor(t, 1, 120)); //ts larger than the first value
		Assertions.assertEquals(0, TimeRangeSearch.findLastFloor(t, 1, 10)); // Have a value
	}

	@Test
	public void findLastFloorRedundantValuesTest() {
		TimeRange[] t = new TimeRange[8];

		t[0] = new POHEntry(3, 1);
		t[1] = new POHEntry(5, 2);
		t[2] = new POHEntry(7, 3);
		t[3] = new POHEntry(7, 4);
		t[4] = new POHEntry(7, 5);
		t[5] = new POHEntry(7, 6);
		t[6] = new POHEntry(9, 7);
		t[7] = new POHEntry(11, 8);

		Assertions.assertEquals(5,  TimeRangeSearch.findLastFloor(t, t.length, 8));
		Assertions.assertEquals(1, TimeRangeSearch.findLastFloor(t, t.length, 6));
		Assertions.assertEquals(5, TimeRangeSearch.findLastFloor(t, t.length, 7)); // Exact match value

		t[6] = new POHEntry(7, 7);
		t[7] = new POHEntry(7, 8); //Matching last values
		Assertions.assertEquals(7,  TimeRangeSearch.findLastFloor(t, t.length, 8));
	}

	@Test
	public void findFirstFloorTest() {
		int count = 9;
		TimeRange[] t = new TimeRange[count];
		int offset = 10;

		for(int i=0; i<count; i++) {
			t[i] = new POHEntry(i*offset+offset, i+1);
		}

		Assertions.assertEquals(0, TimeRangeSearch.findFirstFloor(t, count, 17)); // Have a value
		Assertions.assertEquals(-1, TimeRangeSearch.findFirstFloor(t, count, 9)); //ts lower than the first value
		Assertions.assertEquals(8, TimeRangeSearch.findFirstFloor(t, count, 120)); //ts larger than the first value
	}

	@Test
	public void findFirstFloorRedundantValuesTest() {
		TimeRange[] t = new TimeRange[8];

		t[0] = new POHEntry(3, 1);
		t[1] = new POHEntry(5, 2);
		t[2] = new POHEntry(7, 3);
		t[3] = new POHEntry(7, 4);
		t[4] = new POHEntry(7, 5);
		t[5] = new POHEntry(7, 6);
		t[6] = new POHEntry(9, 7);
		t[7] = new POHEntry(11, 8);

		Assertions.assertEquals(5,  TimeRangeSearch.findFirstFloor(t, t.length, 8));
		Assertions.assertEquals(1, TimeRangeSearch.findFirstFloor(t, t.length, 6));
		Assertions.assertEquals(1, TimeRangeSearch.findFirstFloor(t, t.length, 7)); // Exact match value

		t[6] = new POHEntry(7, 7);
		t[7] = new POHEntry(7, 8); //Matching last values
		Assertions.assertEquals(7,  TimeRangeSearch.findFirstFloor(t, t.length, 8));
	}
}
