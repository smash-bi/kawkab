package kawkab.fs.tests;

import kawkab.fs.api.FileOptions;
import kawkab.fs.api.Record;
import kawkab.fs.commons.Configuration;
import kawkab.fs.core.FileHandle;
import kawkab.fs.core.Filesystem;
import kawkab.fs.core.records.SampleRecord;
import kawkab.fs.core.exceptions.KawkabException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class FileRecordTest {
	@BeforeAll
	public static void initialize() throws IOException, InterruptedException, KawkabException {
		System.out.println("-------------------------------");
		System.out.println("- Initializing the filesystem -");
		System.out.println("-------------------------------");
		
		int nodeID = Configuration.getNodeID();
		Properties props = Configuration.getProperties(Configuration.propsFileCluster);
		Filesystem.bootstrap(nodeID, props);
	}
	
	@AfterAll
	public static void terminate() throws KawkabException, InterruptedException, IOException {
		System.out.println("------------------------------");
		System.out.println("- Terminating the filesystem -");
		System.out.println("------------------------------");
		
		Filesystem.instance().shutdown();
	}
	
	@Test @Disabled
	public void smokeTest() throws IOException, KawkabException, InterruptedException {
		System.out.println("------------------------");
		System.out.println("- Records - Smoke Test -");
		System.out.println("------------------------");
		Random rand = new Random();
		Record rec = new SampleRecord(System.currentTimeMillis(), rand);

		Filesystem fs = Filesystem.instance();
		
		FileHandle file = fs.open("RecordsSmokeTest", Filesystem.FileMode.APPEND, new FileOptions(rec.size()));
		
		file.append(rec.copyOutSrcBuffer(), rec.timestamp(), rec.size());
		
		Record recordOut = new SampleRecord();
		
		file.recordNum(recordOut.copyInDstBuffer(), file.size()/rec.size(), rec.size());
		
		fs.close(file);
		
		assertEquals(rec, recordOut);
	}
	
	@Test
	public void multiBlockTest() throws IOException, KawkabException, InterruptedException {
		System.out.println("------------------------------");
		System.out.println("- Records - Multi Block Test -");
		System.out.println("------------------------------");
		Random rand = new Random();
		int recSize = new SampleRecord().size();
		int toAppend = Configuration.instance().dataBlockSizeBytes/recSize + 3; //Create at least one new block
		List<Record> recs = new ArrayList<>();
		
		Filesystem fs = Filesystem.instance();
		
		FileHandle file = fs.open("RecordsMultiBlockTest", Filesystem.FileMode.APPEND, new FileOptions(recSize));
		
		int nextIndex = (int) (file.size() / recSize) + 1; //The first record number from where we will read

		int offset = (int)file.size()/recSize;

		for (int i=0; i<toAppend; i++) {
			Record rec = new SampleRecord(i+1+offset, rand);

			recs.add(rec);
			file.append(rec.copyOutSrcBuffer(), rec.timestamp(), rec.size());
		}
		
		Record actual = new SampleRecord();
		for (Record expected : recs) {
			file.recordNum(actual.copyInDstBuffer(), nextIndex++, SampleRecord.length());
			assertEquals(expected, actual);
		}
		
		fs.close(file);
	}
	
	@Test @Disabled
	public void simpleRecordReadTest() throws IOException, KawkabException, InterruptedException {
		System.out.println("---------------------------");
		System.out.println("- Simple Record Read Test -");
		System.out.println("---------------------------");
		Random rand = new Random();
		Record rec = new SampleRecord(System.currentTimeMillis(), rand);

		Filesystem fs = Filesystem.instance();
		
		FileHandle file = fs.open("SimpleRecordReadTest", Filesystem.FileMode.APPEND, new FileOptions(rec.size()));
		
		file.append(rec.copyOutSrcBuffer(), rec.timestamp(), rec.size());
		
		Record recordOutNum = new SampleRecord();
		Record recordOutAt = new SampleRecord();
		
		file.recordNum(recordOutNum.copyInDstBuffer(), file.size()/rec.size(), rec.size());
		file.recordAt(recordOutAt.copyInDstBuffer(), rec.timestamp(), rec.size());
		
		fs.close(file);
		
		assertEquals(rec, recordOutNum);
		assertEquals(rec, recordOutAt);
	}

	@Test @Disabled
	public void rangeReadTest() throws IOException, KawkabException, InterruptedException {
		System.out.println("---------------------------");
		System.out.println("- Record Range Read Test -");
		System.out.println("---------------------------");
		Random rand = new Random();

		Filesystem fs = Filesystem.instance();
		FileHandle file = fs.open("rangeReadTest", Filesystem.FileMode.APPEND, new FileOptions(SampleRecord.length()));

		int numRecs = 10;
		int offset = 10;
		Record[] records = new Record[numRecs];

		for (int i=0; i<numRecs; i++) {
			long ts = i*offset+offset;
			Record rec = new SampleRecord(ts, rand);
			records[i] = rec;
			file.append(rec.copyOutSrcBuffer(), rec.timestamp(), rec.size());
		}

		List<Record> results = null;
		results = file.readRecords(1, 13, new SampleRecord()); //Lower limit
		assertEquals(records[0], results.get(0));

		System.out.println();

		results = file.readRecords(95, 105, new SampleRecord()); //Upper limit
		assertEquals(records[9], results.get(0));

		results = file.readRecords(1, 115, new SampleRecord()); //All range covered
		for(int i=0; i<numRecs; i++) {
			assertEquals(records[i], results.get(numRecs-i-1));
		}

		results = file.readRecords(1, 9, new SampleRecord()); //lower out of range
		assertEquals(null, results);

		results = file.readRecords(115, 120, new SampleRecord()); //upper out of range
		assertEquals(null, results);

		results = file.readRecords(11, 19, new SampleRecord()); //middle out of range
		assertEquals(null, results);

		results = file.readRecords(15, 35, new SampleRecord()); //middle covered
		assertEquals(2, results.size());
		assertEquals(records[2], results.get(0));
		assertEquals(records[1], results.get(1));

		fs.close(file);
	}

	@Test @Disabled
	public void rangeReadTestLarge() throws IOException, KawkabException, InterruptedException {
		System.out.println("--------------------------------");
		System.out.println("- Record Range Read Test Large -");
		System.out.println("--------------------------------");
		Random rand = new Random();

		Filesystem fs = Filesystem.instance();
		FileHandle file = fs.open("rangeReadTestLarge", Filesystem.FileMode.APPEND, new FileOptions(SampleRecord.length()));

		int numRecs = 10000;
		int tsOffset = 5;
		Record[] records = new Record[numRecs];

		for (int i = 0; i < numRecs; i++) {
			long ts = i * tsOffset + tsOffset;
			Record rec = new SampleRecord(ts, rand.nextDouble(), rand.nextDouble(), rand.nextBoolean(), rand.nextDouble(), rand.nextDouble(), rand.nextBoolean());
			records[i] = rec;
			file.append(rec.copyOutSrcBuffer(), rec.timestamp(), rec.size());
		}

		// Reading the records from the start
		List<Record> results = file.readRecords(1, 1000, new SampleRecord());
		int expectedLen = 1000/tsOffset;
		for (int i=0; i<expectedLen; i++) {
			assertEquals(records[i], results.get(expectedLen-i-1));
		}

		// Reading the records from the end
		int minRecIdx = 9000;
		int lowerTS = minRecIdx*tsOffset+tsOffset;
		int largeMaxTS = numRecs*tsOffset+2*tsOffset; //Should be larger than the ts of the last record
		results = file.readRecords(lowerTS, largeMaxTS, new SampleRecord()); //keeping maxTS larger than the last rec's ts
		expectedLen = numRecs-minRecIdx;
		for (int i=0; i<expectedLen; i++) {
			assertEquals(records[minRecIdx+i], results.get(expectedLen-i-1));
		}

		// Reading the last record
		lowerTS = (numRecs-1)*tsOffset+tsOffset - 1;
		results = file.readRecords(lowerTS, largeMaxTS, new SampleRecord());
		assertEquals(1, results.size());
		assertEquals(records[numRecs-1], results.get(0));

		// Reading out of range in the middle
		results = file.readRecords(1*tsOffset+tsOffset+1, 2*tsOffset+tsOffset-1, new SampleRecord());
		assertNull(results);

		// Reading records from a middle range
		minRecIdx = 5;
		int maxRecIdx = numRecs-5;
		lowerTS = minRecIdx*tsOffset+tsOffset - 1;
		int higherTS = maxRecIdx*tsOffset+tsOffset + 1;

		long t = System.currentTimeMillis();
		results = file.readRecords(lowerTS, higherTS, new SampleRecord()); //keeping maxTS larger than the last rec's ts

		long diff = System.currentTimeMillis() - t;

		expectedLen = maxRecIdx-minRecIdx + 1;
		for (int i=0; i<expectedLen; i++) {
			assertEquals(records[minRecIdx+i], results.get(expectedLen-i-1));
		}

		double lat = diff*1.0 / expectedLen;

		System.out.println("Lat = " + lat + ", numRecs " + expectedLen);

		fs.close(file);
	}
}
