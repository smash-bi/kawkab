package kawkab.fs.testclient;

import com.google.common.base.Stopwatch;
import kawkab.fs.api.Record;
import kawkab.fs.client.KClient;
import kawkab.fs.core.ApproximateClock;
import kawkab.fs.core.Filesystem;
import kawkab.fs.core.exceptions.KawkabException;
import kawkab.fs.core.exceptions.OutOfMemoryException;
import kawkab.fs.utils.Accumulator;
import kawkab.fs.utils.LatHistogram;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public class TestClient {
	private int id;
	private String sip;
	private int sport;

	private KClient client;
	private Random rand;
	private Printer pr;
	private Record recGen;
	private String cls;

	private static int rampDownSec = 2;

	public TestClient(int id, String sip, int sport, Record recGen, Printer printer) {
		this.id = id;
		this.sip = sip;
		this.sport = sport;

		this.pr = printer;

		rand = new Random();

		this.recGen = recGen;
		cls = String.format("[%d] ",id);
	}

	public Result runAppendTest(int cid, int testDurSec, int nTestFiles, int warmupSecs, int batchSize, TestClientServiceClient rpcClient) throws KawkabException {
		pr.print(cls+"Starting append test...");

		if (client == null || !client.isConnected()) {
			throw new KawkabException("Client is not connected");
		}

		if (batchSize > 1) {
			return appendTestWB(cid, testDurSec, nTestFiles, warmupSecs, batchSize, rpcClient);
		}

		return appendTest(cid, testDurSec, nTestFiles, warmupSecs, rpcClient);
	}

	public Result runReadTest(int cid, int testDurSec, int nTestFiles, int warmupSecs, TestClientServiceClient rpcClient) throws KawkabException {
		pr.print(cls+"Starting read test...");

		if (client == null || !client.isConnected()) {
			throw new KawkabException("Client is not connected");
		}

		return readTest(cid, testDurSec, nTestFiles, warmupSecs, rpcClient);
	}

	public Result runNoopWritesTest(int cid, int testDurSec, int nTestFiles, int warmupSecs, int batchSize, TestClientServiceClient rpcClient) throws KawkabException {
		pr.print(cls+"Starting NoOp test...");

		if (client == null || !client.isConnected()) {
			throw new KawkabException(cls+"Client is not connected");
		}

		return noopWriteTest(cid, testDurSec, nTestFiles, warmupSecs, batchSize, rpcClient);
	}

	private Result readTest(int cid, int testDurSec, int nTestFiles, int warmupSecs, TestClientServiceClient rpcClient) throws KawkabException {
		String[] fnames = openFiles(cid-1, nTestFiles, "testfile", Filesystem.FileMode.READ);

		try {
			System.out.println("Waiting for 5 secs before starting...");
			Thread.sleep(5000); //Sleep for 10 seconds before start reading
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		rpcClient.barrier(id);

		warmupSecs = 10; //FIXME: We should not override the value here

		Result res = null;
		try {
			pr.print(String.format("%s Ramp-up for %d seconds...", cls, warmupSecs));
			readRecords(fnames, warmupSecs, recGen.newRecord());

			pr.print(cls+"Reading records ...");
			res = readRecords(fnames, testDurSec, recGen.newRecord());

			pr.print(String.format("%s Ramp-down for %d seconds...", cls, rampDownSec));
			readRecords(fnames, rampDownSec, recGen.newRecord());
		}catch (OutOfMemoryException | InterruptedException e) {
			e.printStackTrace();
		} catch (Exception | AssertionError e) {
			e.printStackTrace();
		} finally {
			closeFiles(fnames);
		}

		return res;
	}

	private Result appendTest(int cid, int testDurSec, int nTestFiles, int warmupSecs, TestClientServiceClient rpcClient) throws KawkabException {
		String[] fnames = openFiles(cid-1, nTestFiles, "testfile", Filesystem.FileMode.APPEND);

		rpcClient.barrier(id);

		Result res = null;
		try {
				pr.print(String.format("%s Ramp-up for %d seconds...", cls, warmupSecs));
				appendRecs(fnames, warmupSecs, recGen.newRecord());

				pr.print(cls+"Appending record (no batching)...");
				res = appendRecs(fnames, testDurSec, recGen.newRecord());

				pr.print(String.format("%s Ramp-down for %d seconds...", cls, rampDownSec));
				appendRecs(fnames, rampDownSec, recGen.newRecord());

		}catch (OutOfMemoryException e) {
			e.printStackTrace();
		} finally {
			closeFiles(fnames);
		}

		return res;
	}

	private Result appendTestWB(int cid, int testDurSec, int nTestFiles, int warmupSecs, int batchSize, TestClientServiceClient rpcClient) throws KawkabException {
		String[] fnames = openFiles(cid-1, nTestFiles, "testfile", Filesystem.FileMode.APPEND);

		rpcClient.barrier(id);

		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd 'at' HH:mm:ss:SSS");
		Date date = new Date(System.currentTimeMillis());
		System.out.println("Current time: " + formatter.format(date));

		Result result = null;
		try {
			pr.print(String.format("%s Ramp-up for %d seconds...", cls, warmupSecs));
			appendRecords(fnames, warmupSecs, recGen.newRecord(), batchSize);
			//appendRecsBuffered(fnames, warmupSecs, recGen.newRecord(), batchSize);

			pr.print(String.format("%s Appending records for %d secs (with batching)...", cls, testDurSec));
			result = appendRecords(fnames, testDurSec, recGen.newRecord(), batchSize);
			//result = appendRecsBuffered(fnames, testDurSec, recGen.newRecord(), batchSize);

			pr.print(String.format("%s Ramp-down for %d seconds...", cls, rampDownSec));
			appendRecords(fnames, rampDownSec, recGen.newRecord(), batchSize);
			//appendRecsBuffered(fnames, rampDownSec, recGen.newRecord(), batchSize);
		}catch (OutOfMemoryException e) {
			e.printStackTrace();
		} finally {
			closeFiles(fnames);
		}

		return result;
	}

	private Result appendRecsBuffered(String[] fnames, int durSec, Record recGen, int batchSize) throws OutOfMemoryException, KawkabException {
		long now = System.currentTimeMillis();
		long et = now + durSec*1000;

		Record[] batch = new Record[batchSize];
		for (int i=0; i<batchSize; i++) {
			batch[i] = recGen.newRecord();
		}

		LatHistogram tlog = new LatHistogram(TimeUnit.MICROSECONDS, "Append records buffered", 100, 1000000, batchSize);
		Stopwatch sw = Stopwatch.createStarted();
		Accumulator tputLog = new Accumulator(durSec+1);
		ApproximateClock clock = ApproximateClock.instance();
		long startT = clock.currentTime();
		while((now = System.currentTimeMillis()) < et) {
			String fname = fnames[rand.nextInt(fnames.length)];
			for (int i=0; i<batchSize; i++) {
				batch[i].timestamp(now);
			}

			tlog.start();
			client.appendBuffered(fname, batch, recGen.size());
			tlog.end();

			tputLog.put((int)((clock.currentTime()-startT)/1000.0), batchSize);
		}

		sw.stop();

		Accumulator accm = tlog.accumulator();
		long msec = sw.elapsed(TimeUnit.MILLISECONDS);
		return prepareResult(accm, msec, batchSize, recGen.size(), tputLog.histogram());
	}

	private Result readRecords(String[] fnames, int durSec, Record recGen) throws KawkabException, InterruptedException {
		if (durSec <= 0)
			return null;

		long now = System.currentTimeMillis();
		long et = now + durSec*1000;

		LatHistogram tlog = new LatHistogram(TimeUnit.MICROSECONDS, "Append records buffered", 100, 1000000, -1);
		Accumulator tputLog = new Accumulator(durSec+1);
		ApproximateClock clock = ApproximateClock.instance();
		long startT = clock.currentTime();

		int winMs = 3000; // read window size
		int offsetMs = 3000;  //Read window offset. If we don't give an offset, maxTS may not exist in the file.
		Stopwatch sw = Stopwatch.createStarted();
		while((now = System.currentTimeMillis()) < et) {
			long minTs = now - winMs - offsetMs;
			long maxTs = now - offsetMs;
			String fn = fnames[rand.nextInt(fnames.length)];

			tlog.start();

			List<Record> recs = client.readRecords(fn, minTs, maxTs, recGen, true);
			if (recs == null) {
				System.out.print("-");
				continue;
			}

			int numRecs = recs.size();

			tlog.end(numRecs);

			int elapsed = (int)((clock.currentTime()-startT)/1000.0);
			tputLog.put(elapsed, numRecs);


			long sleepMs = winMs - (System.currentTimeMillis() - now);
			//System.out.printf("Read %d recs in %d us, sleep %d ms\n", numRecs, elUS, sleepMs);
			Thread.sleep(sleepMs);

		}
		sw.stop();

		System.out.println("Finished read test");

		return prepareResult(tlog.accumulator(), sw.elapsed(TimeUnit.MILLISECONDS), -1, recGen.size(), tputLog.histogram());

	}

	private Result appendRecords(String[] fnames, int durSec, Record recGen, int batchSize) throws OutOfMemoryException, KawkabException {
		long now = System.currentTimeMillis();
		long et = now + durSec*1000;

		Random rand = new Random();
		String[] files = new String[batchSize];
		Record[] batch = new Record[batchSize];
		for (int i=0; i<batchSize; i++) {
			batch[i] = recGen.newRecord();
			files[i] = fnames[rand.nextInt(fnames.length)];
		}

		Random r2 = new Random(1);
		//int sleepTimeNs = 1750000; // For stable throughput test
		//int sleepTimeNs = 900000; //For buffer overflow test

		int sleepTimeNs = 500000; //For reads
		int burstProb = 0; // For reads

		int sleepNs = sleepTimeNs;
		int burstDur = 3;
		//int burstProb = 20;


		LatHistogram tlog = new LatHistogram(TimeUnit.MICROSECONDS, "Append records buffered", 100, 1000000, batchSize);
		Stopwatch sw = Stopwatch.createStarted();
		Accumulator tputLog = new Accumulator(durSec+1);
		ApproximateClock clock = ApproximateClock.instance();
		long startT = clock.currentTime();
		int elapsed = 0;

		int lastSet = 0;
		while(now < et) {
			if (elapsed > lastSet) {
				lastSet = elapsed;
				if (elapsed % burstDur == 0) {
					int prob = r2.nextInt(100);
					if (prob < burstProb)
						sleepTimeNs = 0;
					else
						sleepTimeNs = sleepNs;
				}
			}

			if (sleepTimeNs > 0) {
				LockSupport.parkNanos(sleepTimeNs + rand.nextInt(30000));
			}

			now = System.currentTimeMillis();
			for (int i=0; i<batchSize; i++) {
				batch[i].timestamp(now);
			}

			tlog.start();
			try {
				client.appendRecords(files, batch);
			} catch (OutOfMemoryException e) {
				System.out.print(".");
				elapsed = (int)((clock.currentTime()-startT)/1000.0);
				continue;
			}
			tlog.end();

			elapsed = (int)((clock.currentTime()-startT)/1000.0);

			tputLog.put(elapsed, batchSize);
		}
		sw.stop();

		return prepareResult(tlog.accumulator(), sw.elapsed(TimeUnit.MILLISECONDS), batchSize, recGen.size(), tputLog.histogram());
	}

	private Result appendRecordsLongBursts(String[] fnames, int durSec, Record recGen, int batchSize) throws OutOfMemoryException, KawkabException {
		long now = System.currentTimeMillis();
		long et = now + durSec*1000;

		Random rand = new Random();
		String[] files = new String[batchSize];
		Record[] batch = new Record[batchSize];
		for (int i=0; i<batchSize; i++) {
			batch[i] = recGen.newRecord();
			files[i] = fnames[rand.nextInt(fnames.length)];
		}

		int sleepTimeNs = 1250000;
		int sb1 = 100;
		int se1 = 200;
		int sb2 = 500;
		int se2 = 750;

		LatHistogram tlog = new LatHistogram(TimeUnit.MICROSECONDS, "Append records buffered", 100, 1000000, batchSize);
		Stopwatch sw = Stopwatch.createStarted();
		Accumulator tputLog = new Accumulator(durSec+1);
		Accumulator latLOg = new Accumulator(durSec+1);
		ApproximateClock clock = ApproximateClock.instance();
		long startT = clock.currentTime();
		int elapsed = 0;
		while((now = System.currentTimeMillis()) < et) {
			if (!(elapsed >= sb1 && elapsed <= se1) && !(elapsed >= sb2 && elapsed <= se2)) {
				LockSupport.parkNanos(sleepTimeNs + rand.nextInt(50000));
			}

			for (int i=0; i<batchSize; i++) {
				batch[i].timestamp(now);
			}

			tlog.start();
			try {
				client.appendRecords(files, batch);
			} catch (KawkabException e) {
				if (e.getMessage().startsWith("Request failed after")) {
					System.out.println("+");
					elapsed = (int)((clock.currentTime()-startT)/1000.0);
					continue;
				}
			}
			tlog.end();

			elapsed = (int)((clock.currentTime()-startT)/1000.0);

			tputLog.put(elapsed, batchSize);
		}
		sw.stop();

		return prepareResult(tlog.accumulator(), sw.elapsed(TimeUnit.MILLISECONDS), batchSize, recGen.size(), tputLog.histogram());
	}

	private Result appendRecs(String[] fnames, int durSec, Record recGen) throws KawkabException {
		long now = System.currentTimeMillis();
		long et = now + durSec*1000;

		Record rec = recGen.newRandomRecord(rand, now);

		LatHistogram tlog = new LatHistogram(TimeUnit.MICROSECONDS, "Append records", 100, 1000000);
		Stopwatch sw = Stopwatch.createStarted();
		Accumulator tputLog = new Accumulator(durSec+1);
		ApproximateClock clock = ApproximateClock.instance();
		long startT = clock.currentTime();
		while((now = System.currentTimeMillis()) < et) {
			rec.timestamp(now);
			String fname = fnames[rand.nextInt(fnames.length)];

			tlog.start();
			client.append(fname, rec);
			tlog.end();

			tputLog.put((int)((clock.currentTime()-startT)/1000.0), 1);
		}
		sw.stop();

		return prepareResult(tlog.accumulator(), sw.elapsed(TimeUnit.MILLISECONDS), 1, recGen.size(), tputLog.histogram());
	}

	private Result noopWriteTest(int cid, int testDurSec, int nTestFiles, int warmupSecs, int batchSize, TestClientServiceClient rpcClient) throws KawkabException {
		String[] fnames = openFiles(cid-1, nTestFiles, "testfile", Filesystem.FileMode.APPEND);

		rpcClient.barrier(id);

		Result result = null;
		try {
			pr.print(String.format("%s Ramp-up for %d seconds...", cls, warmupSecs));
			appendNoops(fnames, warmupSecs, recGen.newRecord(), batchSize);

			pr.print(String.format("%s NoOP appends for %d secs (with batching)...", cls, testDurSec));
			result = appendNoops(fnames, testDurSec, recGen.newRecord(), batchSize);

			pr.print(String.format("%s Ramp-down for %d seconds...", cls, rampDownSec));
			appendNoops(fnames, rampDownSec, recGen.newRecord(), batchSize);
		}catch (OutOfMemoryException e) {
			e.printStackTrace();
		} finally {
			closeFiles(fnames);
		}

		return result;
	}

	private Result appendNoops(String[] fnames, int durSec, Record recGen, int batchSize) throws OutOfMemoryException, KawkabException {
		long now = System.currentTimeMillis();
		long et = now + durSec*1000;

		Random rand = new Random();
		String[] files = new String[batchSize];
		Record[] batch = new Record[batchSize];
		for (int i=0; i<batchSize; i++) {
			batch[i] = recGen.newRecord();
			files[i] = fnames[rand.nextInt(fnames.length)];
		}

		LatHistogram tlog = new LatHistogram(TimeUnit.MICROSECONDS, "NoOP records buffered", 100, 1000000, batchSize);
		Stopwatch sw = Stopwatch.createStarted();
		Accumulator tputLog = new Accumulator(durSec+1);
		ApproximateClock clock = ApproximateClock.instance();
		long startT = clock.currentTime();
		while((now = System.currentTimeMillis()) < et) {
			for (int i=0; i<batchSize; i++) {
				batch[i].timestamp(now);
			}

			tlog.start();
			client.appendNoops(files, batch);
			tlog.end();

			tputLog.put((int)((clock.currentTime()-startT)/1000.0), batchSize);
		}
		sw.stop();

		return prepareResult(tlog.accumulator(), sw.elapsed(TimeUnit.MILLISECONDS), batchSize, recGen.size(), tputLog.histogram());
	}

	/*private Result sendNoops(int durSec) throws KawkabException {
		long et = System.currentTimeMillis() + durSec*1000;

		LatHistogram tlog = new LatHistogram(TimeUnit.MICROSECONDS, "NoOPs test", 100, 1000000);
		Stopwatch sw = Stopwatch.createStarted();
		Accumulator tputLog = new Accumulator(durSec+1);
		ApproximateClock clock = ApproximateClock.instance();
		long startT = clock.currentTime();
		while (System.currentTimeMillis() < et) {
			tlog.start();
			client.noopWrite(0);
			tlog.end();

			tputLog.put((int)((clock.currentTime()-startT)/1000.0), 1);
		}

		sw.stop();

		return prepareResult(tlog.accumulator(), sw.elapsed(TimeUnit.MILLISECONDS), 1, recGen.size(), tputLog.histogram());
	}*/

	private String[] openFiles(int offset, int numFiles, String prefix, Filesystem.FileMode mode) throws KawkabException {
		assert client.isConnected();

		System.out.printf("Opening files: offset=%d, nf=%d, mode=%s\n", offset, numFiles, mode);

		String[] files = new String[numFiles];
		Filesystem.FileMode[] modes = new Filesystem.FileMode[numFiles];
		int[] recSizes = new int[numFiles];

		for (int i=0; i<files.length; i++) {
			//files[i] = String.format("%s-c%d-%d-%d",prefix,id,i+1,rand.nextInt(1000000));
			files[i] = String.format("%s-%d",prefix,numFiles*offset+i);
			modes[i] = mode;
			recSizes[i] = recGen.size();
			//client.open(files[i], mode, recGen.size());
		}

		int n = client.bulkOpen(files, modes, recSizes);

		assert n == numFiles;

		return files;
	}

	private void closeFiles(String[] files) throws KawkabException {
		assert client.isConnected();

		/*for(String fname : files) {
			client.close(fname);
		}*/

		client.bulkClose(files);
	}

	public void connect() throws KawkabException {
		client = new KClient(id);
		client.connect(sip, sport);
	}

	public void disconnect() throws KawkabException {
		client.disconnect();
	}

	private Result prepareResult(Accumulator accm, double durMsec, int batchSize, int recSize, long[] tputLog) {
		long cnt = accm.count();
		double sizeMB = recSize*cnt / (1024.0 * 1024.0);
		double thr = sizeMB * 1000.0 / durMsec;
		double opThr = cnt * 1000.0 / durMsec;

		return new Result(cnt, opThr, thr, accm.min(), accm.max(), accm.histogram(), tputLog, batchSize);
	}
}
