package kawkab.fs.tests;

import java.io.IOException;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import kawkab.fs.api.FileHandle;
import kawkab.fs.api.FileOptions;
import kawkab.fs.commons.Stats;
import kawkab.fs.core.Filesystem;
import kawkab.fs.core.Filesystem.FileMode;
import kawkab.fs.core.exceptions.IbmapsFullException;
import kawkab.fs.core.exceptions.InvalidFileModeException;
import kawkab.fs.core.exceptions.InvalidFileOffsetException;
import kawkab.fs.core.exceptions.KawkabException;
import kawkab.fs.core.exceptions.MaxFileSizeExceededException;
import kawkab.fs.core.exceptions.OutOfMemoryException;

public class AppendTest {

	@Before
	public void initialize() throws IOException, InterruptedException, KawkabException {
		Filesystem.instance().bootstrap();
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

		int numWriters = 1;
		Thread[] workers = new Thread[numWriters];
		final int bufSize = 1 * 100; // Constants.segmentSizeBytes;//8*1024*1024;
		final long dataSize = 15L * 1024 * 1024 * 1024;

		Stats writeStats = new Stats();
		for (int i = 0; i < numWriters; i++) {
			final int id = i;
			workers[i] = new Thread() {
				public void run() {
					try {
						String filename = new String("/home/smash/twpcf-" + id);
						FileOptions opts = new FileOptions();
						FileHandle file = fs.open(filename, FileMode.APPEND, opts);

						System.out.println("Opening file: " + filename + ", current size=" + file.size());

						Random rand = new Random(0);
						long appended = 0;

						final byte[] writeBuf = new byte[bufSize];
						rand.nextBytes(writeBuf);

						long startTime = System.currentTimeMillis();
						while (appended < dataSize) {
							int toWrite = (int) (appended + bufSize <= dataSize ? bufSize : dataSize - appended);
							appended += file.append(writeBuf, 0, toWrite);
						}

						double durSec = (System.currentTimeMillis() - startTime) / 1000.0;
						double sizeMB = appended / (1024.0 * 1024);
						double thr = sizeMB / durSec;
						writeStats.putValue(thr);

						System.out.println(
								String.format("File %d: Data size = %.0fMB, Write thr = %.0fMB/s", id, sizeMB, thr));
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

		System.out.printf("WriteStats: sum=%.0f, %s, bufSize=%d\n", writeStats.sum(), writeStats, bufSize);
	}
}
