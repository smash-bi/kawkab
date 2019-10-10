package kawkab.fs.tests;

import kawkab.fs.api.FileOptions;
import kawkab.fs.api.Record;
import kawkab.fs.commons.Configuration;
import kawkab.fs.core.FileHandle;
import kawkab.fs.core.Filesystem;
import kawkab.fs.core.SampleRecord;
import kawkab.fs.core.exceptions.KawkabException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

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
	
	@Test
	public void smokeTest() throws IOException, KawkabException, InterruptedException {
		System.out.println("------------------------");
		System.out.println("- Records - Smoke Test -");
		System.out.println("------------------------");
		Random rand = new Random();
		Record rec = new SampleRecord(System.currentTimeMillis(),
				rand.nextFloat(), rand.nextFloat(), rand.nextBoolean(),
				rand.nextFloat(), rand.nextFloat(), rand.nextBoolean());
		
		Filesystem fs = Filesystem.instance();
		
		FileHandle file = fs.open("RecordsSmokeTest", Filesystem.FileMode.APPEND, new FileOptions(rec.size()));
		
		file.append(rec);
		
		Record recordOut = new SampleRecord();
		
		file.recordNum(recordOut, file.size()/rec.size());
		
		fs.close(file);
		
		Assertions.assertEquals(rec, recordOut);
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
		
		for (int i=0; i<toAppend; i++) {
			Record rec = new SampleRecord(System.currentTimeMillis(),
					rand.nextFloat(), rand.nextFloat(), rand.nextBoolean(),
					rand.nextFloat(), rand.nextFloat(), rand.nextBoolean());
			
			recs.add(rec);
			file.append(rec);
		}
		
		Record actual = new SampleRecord();
		for (Record expected : recs) {
			file.recordNum(actual, nextIndex++);
			Assertions.assertEquals(expected, actual);
		}
		
		fs.close(file);
	}
	
	@Test
	public void simpleRecordReadTest() throws IOException, KawkabException, InterruptedException {
		System.out.println("---------------------------");
		System.out.println("- Simple Record Read Test -");
		System.out.println("---------------------------");
		Random rand = new Random();
		Record rec = new SampleRecord(System.currentTimeMillis(),
				rand.nextFloat(), rand.nextFloat(), rand.nextBoolean(),
				rand.nextFloat(), rand.nextFloat(), rand.nextBoolean());
		
		Filesystem fs = Filesystem.instance();
		
		FileHandle file = fs.open("SimpleRecordReadTest", Filesystem.FileMode.APPEND, new FileOptions(rec.size()));
		
		file.append(rec);
		
		Record recordOutNum = new SampleRecord();
		Record recordOutAt = new SampleRecord();
		
		file.recordNum(recordOutNum, file.size()/rec.size());
		file.recordAt(recordOutAt, rec.key());
		
		fs.close(file);
		
		Assertions.assertEquals(rec, recordOutNum);
		Assertions.assertEquals(rec, recordOutAt);
	}

	@Test
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
			Record rec = new SampleRecord(ts, rand.nextDouble(), rand.nextDouble(), rand.nextBoolean(), rand.nextDouble(), rand.nextDouble(), rand.nextBoolean());
			records[i] = rec;
			file.append(rec);
		}

		List<Record> results = null;
		results = file.readRecords(1, 13, new SampleRecord()); //Lower limit
		Assertions.assertEquals(records[0], results.get(0));

		System.out.println();

		results = file.readRecords(95, 105, new SampleRecord()); //Upper limit
		Assertions.assertEquals(records[9], results.get(0));

		results = file.readRecords(1, 115, new SampleRecord()); //All range covered
		for(int i=0; i<numRecs; i++) {
			Assertions.assertEquals(records[i], results.get(numRecs-i-1));
		}

		results = file.readRecords(1, 9, new SampleRecord()); //lower out of range
		Assertions.assertEquals(null, results);

		results = file.readRecords(115, 120, new SampleRecord()); //upper out of range
		Assertions.assertEquals(null, results);

		results = file.readRecords(11, 19, new SampleRecord()); //middle out of range
		Assertions.assertEquals(null, results);

		results = file.readRecords(15, 35, new SampleRecord()); //middle covered
		Assertions.assertEquals(2, results.size());
		Assertions.assertEquals(records[2], results.get(0));
		Assertions.assertEquals(records[1], results.get(1));

		fs.close(file);
	}
}
