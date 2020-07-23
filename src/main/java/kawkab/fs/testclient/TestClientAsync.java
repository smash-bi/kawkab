package kawkab.fs.testclient;

import com.google.common.math.Stats;
import kawkab.fs.api.Record;
import kawkab.fs.client.KClient;
import kawkab.fs.core.ApproximateClock;
import kawkab.fs.core.Filesystem;
import kawkab.fs.core.exceptions.KawkabException;
import kawkab.fs.core.exceptions.OutOfMemoryException;
import kawkab.fs.utils.Accumulator;
import kawkab.fs.utils.LatHistogram;
import org.apache.commons.math3.distribution.PoissonDistribution;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public class TestClientAsync {
	private int cid;
	private KClient client;
	private Random fileRand;
	private String[] files;
	//private ApproximateClock clock;
	private Clock clock = Clock.systemDefaultZone();
	private volatile boolean work = true;

	private static int rampDownSec = 2;

	TestClientAsync(int id) {
		this.cid = id;
		fileRand = new Random();
		//clock = ApproximateClock.instance();
	}

	Result[] runTest(boolean isController, double iat, int writeRatio, int testDurSec, int filesPerclient, int apBatchSize,
						   int warmupSecs, Record recGen, final LinkedBlockingQueue<Instant> rq, TestClientServiceClient rpcClient) throws KawkabException {
		int offset = (cid-1)*filesPerclient;
		openFiles(offset, filesPerclient, recGen.size());

		rpcClient.barrier(cid);

		Thread ctrlThr = null;
		if (isController) {
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				e.printStackTrace();
				return null;
			}

			work = true;
			ctrlThr = new Thread(() -> generateReqs(iat, writeRatio, rq));
			ctrlThr.start();
		}

		Result[] res = null;
		try {
			System.out.printf("Ramp-up for %d seconds...\n", warmupSecs);
			test(warmupSecs, recGen.newRecord(), apBatchSize, writeRatio, rq, false, isController);

			System.out.printf("Running test for %d seconds...\n", testDurSec);
			res = test(testDurSec, recGen.newRecord(), apBatchSize, writeRatio, rq, true, isController);

			System.out.printf("Ramp-down for %d seconds...\n", rampDownSec);
			test(rampDownSec, recGen.newRecord(), apBatchSize, writeRatio, rq, false, isController);
		}catch (AssertionError | Exception e) {
			e.printStackTrace();
		} finally {
			closeFiles();
		}

		//rpcClient.barrier(cid);

		if (isController) {
			work = false;
			ctrlThr.interrupt();
			try {
				ctrlThr.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		return res;
	}

	private void generateReqs(final double iat, final int writeRatio, final LinkedBlockingQueue<Instant> rq) {
		PoissonDistribution arrRand = new PoissonDistribution(iat); //arrival time for the next request
		Random waitRand = new Random();

		while(work) {
			int toSend = 1;
			long sleepStartTime = System.nanoTime();
			int toWait = arrRand.sample(); // - (int)lastElapsed + residue;
			int waitTime = toWait;

			while(true) {
				try {
					assert waitTime > 0;

					if (waitTime >= 3 * 1000) {
						Thread.sleep(waitTime / 1000, 0);
					} else if (waitTime > 60) { //It sleeps for at least 60us.
						LockSupport.parkNanos(waitTime*1000);
					} else {
						LockSupport.parkNanos(60000);
					}
				} catch (InterruptedException e) {
					return;
				}

				int sleepTime = (int)((System.nanoTime() - sleepStartTime)/1000.0);
				if (sleepTime > toWait) {
					toSend = sleepTime/toWait;
					int mod = sleepTime%toWait;
					if (waitRand.nextInt(100) < mod)
						toSend += 1;
				} else if (sleepTime < toWait) {
					waitTime = toWait - sleepTime;
					continue;
				}

				break;
			}

			int size = rq.size();
			if (size > 0 && size % 1000 == 0) {
				System.out.printf("Reqs queue %d\n", rq.size());
			} else if (size > 0) {
				System.out.print(size +" ");
			}

			for (int i=0; i<toSend; i++) {
				rq.add(clock.instant());

				/*if ((reqRand.nextInt(100)+1) <= writeRatio) {
					//sendAppendRequest(fnames, apBatchSize, recGen, accm);
					rq.add(clock.instant().getNano());
				} else {
					//sendReadRequest(fnames, recGen, accm);
					rq.add(-1);
				}*/
			}
		}
	}


	private Result[] test(int durSec, Record recGen, final int apBatchSize, final int writeRatio, final LinkedBlockingQueue<Instant> rq,
						  boolean logStats, boolean isController)
			throws OutOfMemoryException, KawkabException {
		long now = System.currentTimeMillis();
		long et = now + durSec*1000;

		Accumulator wLats = new Accumulator(1000000);
		Accumulator rLats = new Accumulator(1000000);
		Accumulator rTputs = new Accumulator(durSec+1);
		Accumulator wTputs = new Accumulator(durSec+1);
		Accumulator rRps = new Accumulator(durSec+1);
		Accumulator wRps = new Accumulator(durSec+1);

		String[] fnames = new String[apBatchSize]; //Random files used in a batch. Actual file name is populated from files class variable.
		Record[] records = new Record[apBatchSize];
		for (int i=0; i<records.length; i++) {
			records[i] = recGen.newRecord();
		}

		LatHistogram wLog = null;
		if (isController)
			wLog = new LatHistogram(TimeUnit.MICROSECONDS, "AppendSendReqLat", 50, 40000);

		long rBatchSize = 0;
		ApproximateClock apClock = ApproximateClock.instance();
		Random reqRand = new Random();
		long startT = System.currentTimeMillis();
		while((now = apClock.currentTime()) < et) {
			try {
				Instant ts = rq.take();
				boolean isAppend = (reqRand.nextInt(100)+1) <= writeRatio;

				int lat;
				int batchSize = -1;
				if (isAppend) {
					if (isController)
						wLog.start();

					sendAppendRequest(fnames, records, now-startT+1);
					batchSize = apBatchSize;

					if (isController)
						wLog.end(1);
				} else {
					batchSize = sendReadRequest(recGen, apClock.currentTime());
					if (batchSize == 0) {
						System.out.print("-");
						continue;
					}
				}

				try {
					lat = (int) (Duration.between(ts, clock.instant()).toNanos() / 1000);
				} catch (ArithmeticException e) {
					continue;
				}

				if (lat < 0)
					continue;

				int elapsed = (int)((apClock.currentTime()-startT)/1000.0);
				if (isAppend) {
					wLats.put(lat, 1);
					wTputs.put(elapsed, 1);
					wRps.put(elapsed, batchSize);
				} else {
					rLats.put(lat, 1);
					rBatchSize += batchSize;
					rTputs.put(elapsed, 1);
					rRps.put(elapsed, batchSize);
				}
			} catch (InterruptedException e) {
				//e.printStackTrace();
				break;
			}
		}

		Result readRes = null;
		Result writeRes = null;

		if (rLats.count() > 0) {
			//rBatchSize = rBatchSize / rLats.count(); // Take the average number of records read per operation
			System.out.println("rBatchSize = " + (rBatchSize/rLats.count()));
			rBatchSize = 1; // Take the average number of records read per operation
			readRes = prepareResult(rLats, rTputs, rRps, recGen.size());
		}

		if (wLats.count() > 0) {
			writeRes = prepareResult(wLats, wTputs, wRps, recGen.size());
		}

		if (isController && logStats) {
			wLog.printStats();
			System.out.println("Print stats on server for file: " + files[0]);
			client.printStats(files[0]);
		}

		return new Result[]{readRes, writeRes};
	}

	private void sendAppendRequest(String[] fnames, Record[] records, long ts) throws KawkabException {
		for (int i=0; i<records.length; i++) {
			records[i].timestamp(ts);
			fnames[i] = files[fileRand.nextInt(files.length)];
		}

		//client.appendRecords(fnames, records);
		client.appendRecordsUnpacked(fnames, records);
	}

	private int sendReadRequest(Record recGen, long tsNow) throws KawkabException {
		int winMs = 1000; // read window size
		int offsetMs = 2000;  //Read window offset. If we don't give an offset, maxTS may not exist in the file.
		long minTs = tsNow - winMs - offsetMs;
		long maxTs = minTs + winMs;
		String fn = files[fileRand.nextInt(files.length)];
		List<Record> res = client.readRecords(fn, minTs, maxTs, recGen, true);
		if (res == null)
			return 0;

		return res.size();
	}

	void openFiles(int offset, int numFiles, int recSize) throws KawkabException {
		assert client.isConnected();
		assert files == null : "Files are already open";

		System.out.printf("Opening files for append: offset=%d, nf=%d, cid=%d\n", offset, numFiles, cid);

		files = new String[numFiles];
		Filesystem.FileMode[] modes = new Filesystem.FileMode[numFiles];
		int[] recSizes = new int[numFiles];

		for (int i=0; i<files.length; i++) {
			files[i] = String.format("tf-%d",offset+i);
			modes[i] = Filesystem.FileMode.APPEND;
			recSizes[i] = recSize;
		}

		int n = client.bulkOpen(files, modes, recSizes);

		assert n == numFiles;
	}

	void closeFiles() throws KawkabException {
		assert client.isConnected();

		assert files != null : "Files are not open";

		client.bulkClose(files);

		files = null;
	}

	public void connect(String sip, int sport) throws KawkabException {
		client = new KClient(cid);
		client.connect(sip, sport);
	}

	public void disconnect() throws KawkabException {
		client.disconnect();
	}

	private Result prepareResult(Accumulator lats, Accumulator tputs, Accumulator rps, int recSize) {
		long cnt = lats.count();
		//double sizeMB = recSize*cnt / (1024.0 * 1024.0);
		//double thr = sizeMB * 1000.0 / durMsec;
		double opThr = Stats.meanOf(tputs.buckets());
		int recsPerSec = (int)Stats.meanOf(rps.buckets());
		double thr = recsPerSec*recSize / (1024.0 * 1024.0);

		return new Result(cnt, opThr, thr, lats.min(), lats.max(), lats.buckets(), tputs.buckets(), recsPerSec);
		//return new Result(cnt, opThr, thr, lats.min(), lats.max(), lats.buckets(), rps.buckets(), recsPerSec);
	}
}
