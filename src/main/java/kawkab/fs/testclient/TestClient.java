package kawkab.fs.testclient;

import com.google.common.base.Stopwatch;
import kawkab.fs.api.Record;
import kawkab.fs.client.KClient;
import kawkab.fs.core.ApproximateClock;
import kawkab.fs.core.Filesystem;
import kawkab.fs.core.exceptions.KawkabException;
import kawkab.fs.core.exceptions.OutOfMemoryException;
import kawkab.fs.utils.Accumulator;
import kawkab.fs.utils.TimeLog;

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

	public TestClient(int id, String sip, int sport, Record recGen, Printer printer) {
		this.id = id;
		this.sip = sip;
		this.sport = sport;

		this.pr = printer;

		rand = new Random();

		this.recGen = recGen;
		cls = String.format("[%d] ",id);
	}

	public Result runTest(int testDurSec, int nTestFiles, int warmupSecs, int batchSize) throws KawkabException {
		pr.print(cls+"Starting append test...");
		
		if (client == null || !client.isConnected()) {
			throw new KawkabException("Client is not connected");
		}

		if (batchSize > 1) {
			return appendTest(testDurSec, nTestFiles, warmupSecs, batchSize);
		}

		return appendTest(testDurSec, nTestFiles, warmupSecs);
	}

	public Result runNoopTest(int testDurSec, int warmupSecs) throws KawkabException {
		pr.print(cls+"Starting NoOp test...");

		if (client == null || !client.isConnected()) {
			throw new KawkabException(cls+"Client is not connected");
		}

		return noopTest(testDurSec, warmupSecs);
	}

	private Result appendTest(int testDurSec, int nTestFiles, int warmupSecs) throws KawkabException {
		String[] fnames = openFiles(nTestFiles, "append", Filesystem.FileMode.APPEND);

		Result res = null;
		try {
				pr.print(String.format("%s Ramp-up for %d seconds...", cls, warmupSecs));
				appendRecs(fnames, warmupSecs, recGen.newRecord());

				pr.print(cls+"Appending record (no batching)...");
				res = appendRecs(fnames, testDurSec, recGen.newRecord());

				pr.print(String.format("%s Ramp-down for %d seconds...", cls, 5));
				appendRecs(fnames, 5, recGen.newRecord());

		}catch (OutOfMemoryException e) {
			e.printStackTrace();
		} finally {
			closeFiles(fnames);
		}

		return res;
	}

	private Result appendTest(int testDurSec, int nTestFiles, int warmupSecs, int batchSize) throws KawkabException {
		String[] fnames = openFiles(nTestFiles, "append", Filesystem.FileMode.APPEND);

		Result result = null;
		try {
			pr.print(String.format("%s Ramp-up for %d seconds...", cls, warmupSecs));
			appendRecords(fnames, warmupSecs, recGen.newRecord(), batchSize);

			pr.print(String.format("%s Appending records for %d secs (with batching)...", cls, testDurSec));
			result = appendRecords(fnames, testDurSec, recGen.newRecord(), batchSize);

			pr.print(String.format("%s Ramp-down for %d seconds...", cls, 5));
			appendRecords(fnames, 5, recGen.newRecord(), batchSize);
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

		TimeLog tlog = new TimeLog(TimeUnit.MICROSECONDS, "Append records buffered", 100);
		Stopwatch sw = Stopwatch.createStarted();
		Accumulator tputLog = new Accumulator();
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

			tputLog.put((int)((clock.currentTime()-startT)/1000.0));
		}

		sw.stop();

		Accumulator accm = tlog.accumulator();
		long msec = sw.elapsed(TimeUnit.MILLISECONDS);
		setTput(accm, msec, batchSize, recGen.size());

		return new Result(accm.count(), accm.opsTput(), accm.dataTput(), accm.min(), accm.max(), accm.histogram(), tputLog.histogram());
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

		TimeLog tlog = new TimeLog(TimeUnit.MICROSECONDS, "Append records buffered", 100);
		Stopwatch sw = Stopwatch.createStarted();
		Accumulator tputLog = new Accumulator();
		ApproximateClock clock = ApproximateClock.instance();
		long startT = clock.currentTime();
		while((now = System.currentTimeMillis()) < et) {
			for (int i=0; i<batchSize; i++) {
				batch[i].timestamp(now);
			}

			tlog.start();
			client.appendRecords(files, batch);
			tlog.end();

			tputLog.put((int)((clock.currentTime()-startT)/1000.0));
		}
		sw.stop();

		Accumulator accm = tlog.accumulator();
		long msec = sw.elapsed(TimeUnit.MILLISECONDS);
		setTput(accm, msec, 1, recGen.size());

		return new Result(accm.count(), accm.opsTput(), accm.dataTput(), accm.min(), accm.max(), accm.histogram(), tputLog.histogram());
	}

	private Result appendRecs(String[] fnames, int durSec, Record recGen) throws KawkabException {
		long now = System.currentTimeMillis();
		long et = now + durSec*1000;

		Record rec = recGen.newRandomRecord(rand, now);

		TimeLog tlog = new TimeLog(TimeUnit.MICROSECONDS, "Append records", 100);
		Stopwatch sw = Stopwatch.createStarted();
		Accumulator tputLog = new Accumulator();
		ApproximateClock clock = ApproximateClock.instance();
		long startT = clock.currentTime();
		while((now = System.currentTimeMillis()) < et) {
			rec.timestamp(now);
			String fname = fnames[rand.nextInt(fnames.length)];

			tlog.start();
			client.append(fname, rec);
			tlog.end();

			tputLog.put((int)((clock.currentTime()-startT)/1000.0));
		}
		sw.stop();

		Accumulator accm = tlog.accumulator();
		long msec = sw.elapsed(TimeUnit.MILLISECONDS);
		setTput(accm, msec, 1, recGen.size());

		return new Result(accm.count(), accm.opsTput(), accm.dataTput(), accm.min(), accm.max(), accm.histogram(), tputLog.histogram());
	}

	private Result noopTest(int testDurSec, int warmupSecs) throws KawkabException {
		pr.print(String.format("%s Ramp-up for %d seconds...", cls, warmupSecs));
		sendNoops(warmupSecs);

		pr.print(cls+"Sending noops ...");
		Result res = sendNoops(testDurSec);

		pr.print(String.format("%s Ramp-down for %d seconds...", cls, warmupSecs));
		sendNoops(warmupSecs);

		return res;
	}

	private Result sendNoops(int durSec) throws KawkabException {
		long et = System.currentTimeMillis() + durSec*1000;

		TimeLog tlog = new TimeLog(TimeUnit.MICROSECONDS, "NoOPs test", 100);
		Stopwatch sw = Stopwatch.createStarted();
		Accumulator tputLog = new Accumulator();
		ApproximateClock clock = ApproximateClock.instance();
		long startT = clock.currentTime();
		while (System.currentTimeMillis() < et) {
			tlog.start();
			client.noopWrite(0);
			tlog.end();

			tputLog.put((int)((clock.currentTime()-startT)/1000.0));
		}

		sw.stop();

		Accumulator accm = tlog.accumulator();
		long msec = sw.elapsed(TimeUnit.MILLISECONDS);
		setTput(accm, msec, 1, recGen.size());

		return new Result(accm.count(), accm.opsTput(), accm.dataTput(), accm.min(), accm.max(), accm.histogram(), tputLog.histogram());
	}

	private String[] openFiles(int numFiles, String prefix, Filesystem.FileMode mode) throws KawkabException {
		assert client.isConnected();

		String[] files = new String[numFiles];

		for (int i=0; i<files.length; i++) {
			files[i] = String.format("%s-c%d-%d-%d",prefix,id,i+1,rand.nextInt(10000000));
			client.open(files[i], mode, recGen.size());
		}

		return files;
	}

	private void closeFiles(String[] files) throws KawkabException {
		assert client.isConnected();

		for(String fname : files) {
			client.close(fname);
		}
	}

	public void connect() throws KawkabException {
		client = new KClient(id);
		client.connect(sip, sport);
	}

	public void disconnect() throws KawkabException {
		client.disconnect();
	}

	private void setTput(Accumulator accm, double durMsec, int batchSize, int recSize) {
		long cnt = accm.count()*batchSize;
		double sizeMB = recSize*cnt / (1024.0 * 1024.0);
		double thr = sizeMB * 1000.0 / durMsec;
		double opThr = cnt * 1000.0 / durMsec;
		accm.setDataTput(thr);
		accm.setOpsTput(opThr);
	}
}
