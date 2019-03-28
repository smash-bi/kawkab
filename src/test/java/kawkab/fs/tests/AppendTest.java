package kawkab.fs.tests;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import kawkab.fs.api.FileOptions;
import kawkab.fs.commons.Stats;
import kawkab.fs.core.DataSegment;
import kawkab.fs.core.FileHandle;
import kawkab.fs.core.Filesystem;
import kawkab.fs.core.Filesystem.FileMode;
import kawkab.fs.core.exceptions.AlreadyConfiguredException;
import kawkab.fs.core.exceptions.IbmapsFullException;
import kawkab.fs.core.exceptions.InvalidFileModeException;
import kawkab.fs.core.exceptions.InvalidFileOffsetException;
import kawkab.fs.core.exceptions.KawkabException;
import kawkab.fs.core.exceptions.MaxFileSizeExceededException;
import kawkab.fs.core.exceptions.OutOfMemoryException;

public class AppendTest {

	@Before
	public void initialize() throws IOException, InterruptedException, KawkabException, AlreadyConfiguredException {
		int nodeID = getNodeID();
		Properties props = getProperties();
		
		Filesystem.bootstrap(nodeID, props);
	}
	
	@After
	public void terminate() throws KawkabException, InterruptedException, IOException {
		Filesystem.instance().shutdown();
	}
	
	@Test
	public void appendPerformanceConcurrentFilesTest()
			throws IbmapsFullException, OutOfMemoryException, MaxFileSizeExceededException, InvalidFileOffsetException,
			InvalidFileModeException, IOException, KawkabException, InterruptedException {
		System.out.println("----------------------------------------------------------------");
		System.out.println("            Append Perofrmance Test - Concurrent Files");
		System.out.println("----------------------------------------------------------------");

		Filesystem fs = Filesystem.instance();

		final int numWriters = Integer.parseInt(System.getProperty("numWriters", "1"));
		Thread[] workers = new Thread[numWriters];
		final int bufSize = Integer.parseInt(System.getProperty("bufferSize", "100"));
		final long dataSize = Long.parseLong(System.getProperty("dataSize", "5368709120"));

		Stats writeStats = new Stats();
		Stats opStats = new Stats();
		for (int i = 0; i < numWriters; i++) {
			final int id = i;
			workers[i] = new Thread("Appender-"+id) {
				public void run() {
					try {
						String filename = new String("/home/smash/twpcf-" + id);
						FileOptions opts = new FileOptions();
						
						System.out.println("Opening file: " + filename);
						
						FileHandle file = fs.open(filename, FileMode.APPEND, opts);

						Random rand = new Random(0);
						long appended = 0;

						final byte[] writeBuf = new byte[bufSize];
						rand.nextBytes(writeBuf);

						long startTime = System.currentTimeMillis();
						int toWrite = bufSize;
						long ops = 0;
						while (appended < dataSize) {
							if (dataSize-appended < bufSize)
								toWrite = (int)(dataSize - appended);
							
							appended += file.append(writeBuf, 0, toWrite);
							ops++;
						}
						
						double durSec = (System.currentTimeMillis() - startTime) / 1000.0;
						double sizeMB = appended / (1024.0 * 1024.0);
						double thr = sizeMB / durSec;
						double opThr = ops / durSec;
						writeStats.putValue(thr);
						opStats.putValue(opThr);
						
						file.close();

						System.out.printf("Writer %d: Data size = %.0fMB, Write tput = %,.0f MB/s, Ops tput = %,.0f OPS\n", id, sizeMB, thr, opThr);
					} catch (Exception e) {
						e.printStackTrace();
						return;
					}
				}
			};

			workers[i].start();
		}

		for (int i = 0; i < workers.length; i++) {
			try {
				workers[i].join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		System.out.printf("\n\nWriters stats: sum=%.0f, %s, buffer size=%d, numWriters=%d\nOps stats: %s\n\n", writeStats.sum(), writeStats, bufSize, numWriters, opStats);
	}
	
	private Properties getProperties() throws IOException {
		String propsFile = "/config.properties";
		
		try (InputStream in = Thread.class.getResourceAsStream(propsFile)) {
			Properties props = new Properties();
			props.load(in);
			return props;
		}
	}
	
	private int getNodeID() throws KawkabException {
		String nodeIDProp = "nodeID";
		
		if (System.getProperty(nodeIDProp) == null) {
			throw new KawkabException("System property nodeID is not defined.");
		}
		
		return Integer.parseInt(System.getProperty(nodeIDProp));
	}
}
