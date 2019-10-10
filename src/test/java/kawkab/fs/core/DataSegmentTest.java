package kawkab.fs.core;

import kawkab.fs.api.Record;
import kawkab.fs.commons.Configuration;
import kawkab.fs.core.exceptions.KawkabException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class DataSegmentTest {
	@BeforeAll
	public static void initialize() throws IOException, InterruptedException, KawkabException {
		System.out.println("-------------------------------");
		System.out.println("- Initializing the filesystem -");
		System.out.println("-------------------------------");

		int nodeID = Configuration.getNodeID();
		Properties props = Configuration.getProperties(Configuration.propsFileCluster);
		Configuration.configure(nodeID, props);
	}

	@Test
	public void readAllSmokeTest() throws IOException {
		System.out.println("Test: readAllSmokeTest");
		DataSegmentID id = new DataSegmentID(1, 0, 0);
		DataSegment seg = new DataSegment(id);

		Record rec1 = new SampleRecord(5, 6, 7, false, 8, 9, false);
		int recSize = rec1.size();
		seg.append(rec1.copyOutSrcBuffer(), 0, recSize);

		Record rec2 = new SampleRecord(8, 6, 7, false, 8, 9, false);
		seg.append(rec2.copyOutSrcBuffer(), recSize, recSize);

		Record rec3 = new SampleRecord(10, 6, 7, false, 8, 9, false);
		seg.append(rec3.copyOutSrcBuffer(), recSize*2, recSize);

		List<Record> results = new ArrayList<>();
		seg.readAll(1, 5, new SampleRecord(), results, recSize);
		Assertions.assertEquals(rec1, results.get(0));

		results = new ArrayList<>();
		seg.readAll(6, 9, new SampleRecord(), results, recSize);
		Assertions.assertEquals(rec2, results.get(0));
	}

	@Test
	public void readAllTest() throws IOException {
		System.out.println("Test: readAllTest");
		DataSegmentID id = new DataSegmentID(1, 0, 0);
		DataSegment seg = new DataSegment(id);
		int recSize = SampleRecord.length();

		int numRecs = 10;
		Record[] records = new Record[numRecs];
		int offset = 10;

		for (int i=0; i<numRecs; i++) {
			long ts = i*offset+offset;
			Record rec = new SampleRecord(ts, i, i, false, i, i, false);
			records[i] = rec;
			seg.append(rec.copyOutSrcBuffer(), i*recSize, recSize);
		}

		List<Record> results = new ArrayList<>();
		seg.readAll(1, 13, new SampleRecord(), results, recSize); //Lower limit
		Assertions.assertEquals(records[0], results.get(0));

		results = new ArrayList<>();
		seg.readAll(95, 105, new SampleRecord(), results, recSize); //Upper limit
		Assertions.assertEquals(records[9], results.get(0));

		results = new ArrayList<>();
		seg.readAll(1, 115, new SampleRecord(), results, recSize); //All range covered
		for(int i=0; i<numRecs; i++) {
			Assertions.assertEquals(records[i], results.get(numRecs-i-1));
		}

		results = new ArrayList<>();
		seg.readAll(1, 9, new SampleRecord(), results, recSize); //lower out of range
		Assertions.assertEquals(0, results.size());

		results = new ArrayList<>();
		seg.readAll(115, 120, new SampleRecord(), results, recSize); //upper out of range
		Assertions.assertEquals(0, results.size());

		results = new ArrayList<>();
		seg.readAll(11, 19, new SampleRecord(), results, recSize); //middle out of range
		Assertions.assertEquals(0, results.size());

		results = new ArrayList<>();
		seg.readAll(15, 35, new SampleRecord(), results, recSize); //middle covered
		Assertions.assertEquals(2, results.size());
		Assertions.assertEquals(records[2], results.get(0));
		Assertions.assertEquals(records[1], results.get(1));

	}
}
