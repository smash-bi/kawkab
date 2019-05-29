package kawkab.fs.tests;

import kawkab.fs.api.FileOptions;
import kawkab.fs.api.Record;
import kawkab.fs.commons.Configuration;
import kawkab.fs.core.FileHandle;
import kawkab.fs.core.Filesystem;
import kawkab.fs.core.SampleRecord;
import kawkab.fs.core.exceptions.KawkabException;
import org.junit.jupiter.api.*;

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
}
