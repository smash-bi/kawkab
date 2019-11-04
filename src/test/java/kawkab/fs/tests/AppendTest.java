package kawkab.fs.tests;

import kawkab.fs.api.FileOptions;
import kawkab.fs.api.Record;
import kawkab.fs.commons.Configuration;
import kawkab.fs.commons.Stats;
import kawkab.fs.core.Cache;
import kawkab.fs.core.FileHandle;
import kawkab.fs.core.Filesystem;
import kawkab.fs.core.Filesystem.FileMode;
import kawkab.fs.core.Inode;
import kawkab.fs.core.exceptions.KawkabException;
import kawkab.fs.core.records.SampleRecord;
import kawkab.fs.utils.TimeLog;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Random;

public class AppendTest {

	@BeforeAll
	public static void initialize() throws IOException, InterruptedException, KawkabException {
		int nodeID = Configuration.getNodeID();
		Properties props = Configuration.getProperties(Configuration.propsFileCluster);
		Filesystem.bootstrap(nodeID, props);
	}
	
	@AfterAll
	public static void terminate() throws KawkabException, InterruptedException, IOException {
		Filesystem.instance().shutdown();
	}

	@Test
	public void appendRecordsSingleFileTest() throws IOException, KawkabException, InterruptedException {
		System.out.println("----------------------------------------------------------------");
		System.out.println("       Append Performance Test (records, single Files)");
		System.out.println("----------------------------------------------------------------");

		final Filesystem fs = Filesystem.instance();
		String filename = "/home/smash/arsft-"+ Configuration.instance().thisNodeID;
		Record recFactory = new SampleRecord();
		Random rand = new Random();
		int numRecords = 10000000;
		int tsOffset = 10;
		Stats writeStats = new Stats();
		Stats opStats = new Stats();

		appendRecords(fs, filename, recFactory, rand, numRecords, tsOffset, writeStats, opStats);

		System.out.printf("\n\nWriters stats: sum=%.0f, %s, record size=%d\nOps stats: %s\n\n", writeStats.sum(), writeStats, recFactory.size(), opStats);
	}

	private void appendRecords(Filesystem fs, String filename, Record recFactory, Random rand, int numRecords, int tsOffset, Stats writeStats, Stats opStats)
			throws KawkabException, IOException, InterruptedException {
		int recSize = recFactory.size();
		System.out.println("Opening file: " + filename);
		FileHandle file = fs.open(filename, FileMode.APPEND, new FileOptions(recSize));

		Record[] recs = new Record[numRecords];
		for (int i=0; i<numRecords; i++) {
			long ts = (i+1)*tsOffset;
			recs[i] = recFactory.newRandomRecord(rand, ts);
		}

		long startTime = System.currentTimeMillis();
		TimeLog tlog = new TimeLog(TimeLog.TimeLogUnit.NANOS, "Record append", 5);
		for (int i=0; i<numRecords; i++) {
			tlog.start();
			Record rec = recs[i];
			file.append(rec.copyOutSrcBuffer(), rec.timestamp(), rec.size());
			tlog.end();
		}
		double durSec = (System.currentTimeMillis() - startTime) / 1000.0;

		double sizeMB = numRecords * recSize / (1024.0 * 1024.0);
		double thr = sizeMB / durSec;
		double opThr = numRecords / durSec;
		writeStats.putValue(thr);
		opStats.putValue(opThr);

		System.out.printf("Record write results: Rec. size=%d, count=%d, Data size=%.0fMB, W tput=%,.0f MB/s, Ops tput=%,.0fOPS, tlog: %s\n", recSize, numRecords, sizeMB, thr, opThr, tlog.getStats());

		fs.close(file);
	}
	
	@Test @Disabled
	public void appendBytesTest() throws IOException, KawkabException {
		System.out.println("----------------------------------------------------------------");
		System.out.println("       Append Performance Test (bytes, concurrent Files)");
		System.out.println("----------------------------------------------------------------");
		
		//warmup();

		final Filesystem fs = Filesystem.instance();

		final int numWriters = Integer.parseInt(System.getProperty("numWriters", "1"));
		Thread[] workers = new Thread[numWriters];
		final int bufSize = Integer.parseInt(System.getProperty("bufferSize", "100"));
		final long dataSize = Long.parseLong(System.getProperty("dataSize", "5368709120"));

		System.gc();
		
		Stats writeStats = new Stats();
		Stats opStats = new Stats();
		for (int i = 0; i < numWriters; i++) {
			final int id = i;
			workers[i] = new Thread("Appender-"+id) {
				public void run() {
					try {
						String filename = "/home/smash/twpcf-"+ Configuration.instance().thisNodeID+"-" + id;
						
						System.out.println("Opening file: " + filename);
						
						FileHandle file = fs.open(filename, FileMode.APPEND, FileOptions.defaults());

						Random rand = new Random(0);
						long appended = 0;

						final byte[] writeBuf = new byte[bufSize];
						rand.nextBytes(writeBuf);
						
						TimeLog tlog = new TimeLog(TimeLog.TimeLogUnit.NANOS, "Main append", 5);
						long startTime = System.currentTimeMillis();
						int toWrite = bufSize;
						long ops = 0;
						while (appended < dataSize) {
							if (dataSize-appended < bufSize)
								toWrite = (int)(dataSize - appended);
							
							tlog.start();
							appended += file.append(writeBuf, 0, toWrite);
							tlog.end();

							ops++;
						}
						
						double durSec = (System.currentTimeMillis() - startTime) / 1000.0;
						double sizeMB = appended / (1024.0 * 1024.0);
						double thr = sizeMB / durSec;
						double opThr = ops / durSec;
						writeStats.putValue(thr);
						opStats.putValue(opThr);
						
						fs.close(file);

						System.out.printf("Writer %d: Data size = %.0fMB, Write tput = %,.0f MB/s, Ops tput = %,.0f OPS, tlog %s\n", id, sizeMB, thr, opThr, tlog.getStats());
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			};

			workers[i].start();
		}

		for (Thread worker : workers) {
			try {
				worker.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		System.out.printf("\n\nWriters stats: sum=%.0f, %s, buffer size=%d, numWriters=%d\nOps stats: %s\n\n", writeStats.sum(), writeStats, bufSize, numWriters, opStats);
	}
	
	private void warmup() throws KawkabException, InterruptedException, IOException {
		System.out.println("Warming up ...");
		long st = System.currentTimeMillis();
		
		Filesystem fs = Filesystem.instance();
		FileHandle file = fs.open("appendTestWarmup", FileMode.APPEND, FileOptions.defaults());
		
		long dataSize = 5000 * 1048576; //~5GB
		int bufSize = 10;
		
		Random rand = new Random(0);
		long appended = 0;
		
		final byte[] writeBuf = new byte[bufSize];
		rand.nextBytes(writeBuf);
		
		int toWrite = bufSize;
		while (appended < dataSize) {
			if (dataSize-appended < bufSize)
				toWrite = (int)(dataSize - appended);
			appended += file.append(writeBuf, 0, toWrite);
		}
		
		fs.close(file);
		
		long durSec = (System.currentTimeMillis() - st)/1000;
		
		fs.getTimerQueue().waitUntilEmpty();
		
		Cache.instance().flush(); //Clear the cache for the actual append test.
		
		System.out.printf("Warmup took %d seconds.\n",durSec);
		
	}
	
	private Properties getProperties() throws IOException {
		String propsFile = "/config.properties";
		
		try (InputStream in = Thread.class.getResourceAsStream(propsFile)) {
			Properties props = new Properties();
			props.load(in);
			return props;
		}
	}
	
	public static void main(String args[]) throws InterruptedException, KawkabException, IOException {
		AppendTest test = new AppendTest();
		test.initialize();
		test.appendBytesTest();
		test.terminate();
	}
}
