package kawkab.fs.testclient;

import com.google.common.math.Stats;
import kawkab.fs.api.Record;
import kawkab.fs.client.KClient;
import kawkab.fs.core.ApproximateClock;
import kawkab.fs.core.Filesystem;
import kawkab.fs.core.exceptions.KawkabException;
import kawkab.fs.core.exceptions.OutOfMemoryException;
import kawkab.fs.utils.AccumulatorMap;
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
	private Random reqRand;
	private String[] files;
	//private ApproximateClock clock;
	private Clock clock = Clock.systemDefaultZone();
	private volatile boolean work = true;

	private static final int rampDownSec = 2;
	//private long readRecordTS = 1;

	private long[] timestamps;

	TestClientAsync(int id) {
		this.cid = id;
		fileRand = new Random();
		reqRand = new Random();
		//clock = ApproximateClock.instance();
	}

	Result[] runTest(boolean isController, double repRateMPS, int writeRatio, int totalClients, int clientsPerMachine, int testDurSec, int filesPerclient, int batchSize,
						   int warmupSecs, Record recGen, final LinkedBlockingQueue<Instant> rq, TestClientServiceClient rpcClient) throws KawkabException {
		int offset = (cid-1)*filesPerclient;
		openFiles(offset, filesPerclient, recGen.size(), writeRatio);
		timestamps = new long[files.length];

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
			ctrlThr = new Thread(() -> generateReqs(repRateMPS, totalClients, clientsPerMachine, batchSize, rq));
			ctrlThr.start();
		}

		Result[] res = null;
		try {
			System.out.printf("Ramp-up for %d seconds...\n", warmupSecs);
			test(warmupSecs, recGen.newRecord(), batchSize, writeRatio, rq, false, isController);

			System.out.printf("Running test for %d seconds...\n", testDurSec);
			res = test(testDurSec, recGen.newRecord(), batchSize, writeRatio, rq, true, isController);

			System.out.printf("Ramp-down for %d seconds...\n", rampDownSec);
			test(rampDownSec, recGen.newRecord(), batchSize, writeRatio, rq, false, isController);
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

	private void generateReqs(final double reqRateMPS, int totalCleints, int clientsPerMachine,
							  int batchSize, final LinkedBlockingQueue<Instant> rq) {
		double targetMPS = reqRateMPS/((double)totalCleints/(double)clientsPerMachine)/(double)batchSize; //total / ratePerMachine / batchSize
		double iatMicros = 1 / (targetMPS);  //Convert millions per second to nano second wait interval

		System.out.printf("IAT Micros = %f, ratePerMachMPS  = %f, tc=%d, cpm=%d, bs=%d, mps=%f\n",
				iatMicros, targetMPS, totalCleints, clientsPerMachine, batchSize, reqRateMPS);

		PoissonDistribution arrRand = new PoissonDistribution(iatMicros); //arrival time for the next request

		while(work) {
			int toSend = 1;
			int waitTimeMicros = arrRand.sample();

			if (waitTimeMicros > 60) { //It sleeps for at least 60us.
				LockSupport.parkNanos(waitTimeMicros*1000);
			} else {
				busyWaitMicros(waitTimeMicros);
			}

			int size = rq.size();
			if ((size % 100) == 1) {
				System.out.print(size +" ");
			}

			for (int i=0; i<toSend; i++) {
				rq.add(clock.instant());
			}
		}
	}


	private Result[] test(int durSec, Record recGen, final int reqBatchSize, final int writeRatio, final LinkedBlockingQueue<Instant> rq,
						  boolean logStats, boolean isController)
			throws OutOfMemoryException, KawkabException {
		long now = System.currentTimeMillis();
		long et = now + durSec*1000;

		AccumulatorMap wLats = new AccumulatorMap(1000000);
		AccumulatorMap rLats = new AccumulatorMap(1000000);
		AccumulatorMap rTputs = new AccumulatorMap(durSec+1);
		AccumulatorMap wTputs = new AccumulatorMap(durSec+1);
		AccumulatorMap rRps = new AccumulatorMap(durSec+1);
		AccumulatorMap wRps = new AccumulatorMap(durSec+1);

		String[] fnames = new String[reqBatchSize]; //Random files used in a batch. Actual file name is populated from files class variable.
		//int[] timestamps = new int[reqBatchSize];
		Record[] records = new Record[reqBatchSize];
		for (int i=0; i<records.length; i++) {
			records[i] = recGen.newRandomRecord(new Random(), 0);
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
				//ts = clock.instant();

				boolean isAppend = (reqRand.nextInt(100)+1) <= writeRatio;

				int batchSize = -1;
				if (isAppend) {
					if (isController)
						wLog.start();

					sendAppendRequest(fnames, records, timestamps);
					batchSize = reqBatchSize;

					if (isController)
						wLog.end(1);
				} else {
					//batchSize = sendReadRequest(recGen, apClock.currentTime());

					batchSize = sendFixedWindowReadRequest(recGen, reqBatchSize, timestamps);

					//batchSize = sendHistoricalReadRequest(recGen, readRecordTS, readRecordTS+reqBatchSize);
					//readRecordTS += batchSize;

					if (batchSize == 0) {
						System.out.print("-");
						continue;
					}
				}

				int lat;
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
			//rBatchSize = 1; // Take the average number of records read per operation
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

	private void sendAppendRequest(String[] fnames, Record[] records, long[] timestamps) throws KawkabException {
		for (int i=0; i<records.length; i++) {
			int iFile = fileRand.nextInt(files.length);
			fnames[i] = files[iFile];
			records[i].timestamp(++timestamps[iFile]);

			//if (iFile == 0 && cid == 0)
			//	System.out.printf("cid=%d, file=%s, TS=%d\n", cid, fnames[i], records[i].timestamp());
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

	private int sendFixedRandomWindowReadRequest(Record recGen, int batchSize, long[] timestamps) throws KawkabException {
		int iFile = fileRand.nextInt(files.length);
		String fn = files[iFile];
		long maxLimit = timestamps[iFile];
		if (maxLimit < batchSize)
			return 0;

		int winSizeRecords = 10000;
		long minLimit = maxLimit - winSizeRecords;
		if (minLimit < 0) minLimit = 0;

		//System.out.printf("minTs=%d, maxTs=%d\n", minTs, maxTs);

		long minTS = minLimit + reqRand.nextInt(winSizeRecords);
		if (minTS + batchSize > maxLimit) minTS = maxLimit - batchSize;

		long maxTS = minTS + batchSize;

		List<Record> res = client.readRecords(fn, minTS, maxTS, recGen, true);
		if (res == null)
			return 0;

		return res.size();
	}

	private int sendFixedWindowReadRequest(Record recGen, int batchSize, long[] timestamps) throws KawkabException {
		int iFile = fileRand.nextInt(files.length);
		String fn = files[iFile];
		long maxTs = timestamps[iFile];
		if (maxTs < batchSize)
			return 0;

		long minTs = maxTs - batchSize;

		//System.out.printf("minTs=%d, maxTs=%d\n", minTs, maxTs);

		List<Record> res = client.readRecords(fn, minTs, maxTs, recGen, true);
		if (res == null)
			return 0;

		return res.size();
	}

	private int sendHistoricalReadRequest(Record recGen, long minTs, long maxTs) throws KawkabException {
		String fn = files[fileRand.nextInt(files.length)];
		List<Record> res = client.readRecords(fn, minTs, maxTs, recGen, false);
		if (res == null)
			return 0;

		return res.size();
	}

	void openFiles(int offset, int numFiles, int recSize, int writeRatio) throws KawkabException {
		assert client.isConnected();
		assert files == null : "Files are already open";

		System.out.printf("Opening files for append: offset=%d, nf=%d, cid=%d\n", offset, numFiles, cid);

		files = new String[numFiles];
		Filesystem.FileMode[] modes = new Filesystem.FileMode[numFiles];
		int[] recSizes = new int[numFiles];

		for (int i=0; i<files.length; i++) {
			files[i] = String.format("tf-%d",offset+i);
			if (writeRatio > 0)
				modes[i] = Filesystem.FileMode.APPEND;
			else
				modes[i] = Filesystem.FileMode.READ;
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

	private Result prepareResult(AccumulatorMap lats, AccumulatorMap tputs, AccumulatorMap rps, int recSize) {
		long cnt = lats.count();
		//double sizeMB = recSize*cnt / (1024.0 * 1024.0);
		//double thr = sizeMB * 1000.0 / durMsec;
		double opThr = Stats.meanOf(tputs.sortedBucketVals());
		int recsPerSec = (int)Stats.meanOf(rps.sortedBucketVals());
		double thr = recsPerSec*recSize / (1024.0 * 1024.0);

		return new Result(cnt, opThr, thr, lats.min(), lats.max(), lats, tputs, recsPerSec);
		//return new Result(cnt, opThr, thr, lats.min(), lats.max(), lats.buckets(), rps.buckets(), recsPerSec);
	}

	private void busyWaitMicros(long micros){
		long waitUntil = System.nanoTime() + (micros * 1_000);
		while(waitUntil > System.nanoTime()){ ; }
	}

	private void waitMicros(int micros) {

	}
}
