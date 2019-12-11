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
import org.apache.log4j.pattern.BridgePatternConverter;

import java.util.Random;
import java.util.concurrent.TimeUnit;

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

	public Result runTest(int testDurSec, int nTestFiles, int warmupSecs, int batchSize, TestClientServiceClient rpcClient) throws KawkabException {
		pr.print(cls+"Starting append test...");
		
		if (client == null || !client.isConnected()) {
			throw new KawkabException("Client is not connected");
		}

		if (batchSize > 1) {
			return appendTest(testDurSec, nTestFiles, warmupSecs, batchSize, rpcClient);
		}

		return appendTest(testDurSec, nTestFiles, warmupSecs, rpcClient);
	}

	public Result runNoopWritesTest(int testDurSec, int nTestFiles, int warmupSecs, int batchSize, TestClientServiceClient rpcClient) throws KawkabException {
		pr.print(cls+"Starting NoOp test...");

		if (client == null || !client.isConnected()) {
			throw new KawkabException(cls+"Client is not connected");
		}

		return noopWriteTest(testDurSec, nTestFiles, warmupSecs, batchSize, rpcClient);
	}

	private Result appendTest(int testDurSec, int nTestFiles, int warmupSecs, TestClientServiceClient rpcClient) throws KawkabException {
		String[] fnames = openFiles(nTestFiles, "append", Filesystem.FileMode.APPEND);

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

	private Result appendTest(int testDurSec, int nTestFiles, int warmupSecs, int batchSize, TestClientServiceClient rpcClient) throws KawkabException {
		String[] fnames = openFiles(nTestFiles, "append", Filesystem.FileMode.APPEND);

		rpcClient.barrier(id);

		Result result = null;
		try {
			pr.print(String.format("%s Ramp-up for %d seconds...", cls, warmupSecs));
			//appendRecords(fnames, warmupSecs, recGen.newRecord(), batchSize);
			appendRecsBuffered(fnames, warmupSecs, recGen.newRecord(), batchSize);

			pr.print(String.format("%s Appending records for %d secs (with batching)...", cls, testDurSec));
			//result = appendRecords(fnames, testDurSec, recGen.newRecord(), batchSize);
			result = appendRecsBuffered(fnames, testDurSec, recGen.newRecord(), batchSize);

			pr.print(String.format("%s Ramp-down for %d seconds...", cls, rampDownSec));
			//appendRecords(fnames, rampDownSec, recGen.newRecord(), batchSize);
			appendRecsBuffered(fnames, rampDownSec, recGen.newRecord(), batchSize);
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

		LatHistogram tlog = new LatHistogram(TimeUnit.MICROSECONDS, "Append records buffered", 100, 1000000, batchSize);
		Stopwatch sw = Stopwatch.createStarted();
		Accumulator tputLog = new Accumulator(durSec+1);
		ApproximateClock clock = ApproximateClock.instance();
		long startT = clock.currentTime();
		while((now = System.currentTimeMillis()) < et) {
			for (int i=0; i<batchSize; i++) {
				batch[i].timestamp(now);
			}

			tlog.start();
			client.appendRecords(files, batch);
			tlog.end();

			tputLog.put((int)((clock.currentTime()-startT)/1000.0), batchSize);
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

	private Result noopWriteTest(int testDurSec, int nTestFiles, int warmupSecs, int batchSize, TestClientServiceClient rpcClient) throws KawkabException {
		String[] fnames = openFiles(nTestFiles, "append", Filesystem.FileMode.APPEND);

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

	private String[] openFiles(int numFiles, String prefix, Filesystem.FileMode mode) throws KawkabException {
		assert client.isConnected();

		String[] files = new String[numFiles];
		Filesystem.FileMode[] modes = new Filesystem.FileMode[numFiles];
		int[] recSizes = new int[numFiles];

		for (int i=0; i<files.length; i++) {
			files[i] = String.format("%s-c%d-%d-%d",prefix,id,i+1,rand.nextInt(1000000));
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
