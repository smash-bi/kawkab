package kawkab.fs.testclient;

import kawkab.fs.api.Record;
import kawkab.fs.client.KClient;
import kawkab.fs.core.ApproximateClock;
import kawkab.fs.core.Filesystem;
import kawkab.fs.core.exceptions.KawkabException;
import kawkab.fs.core.exceptions.OutOfMemoryException;
import kawkab.fs.utils.Accumulator;
import kawkab.fs.utils.AccumulatorMap;
import kawkab.fs.utils.LatHistogram;
import org.apache.commons.math3.distribution.PoissonDistribution;

import java.text.SimpleDateFormat;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
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

	private static Instant nullInstant = Instant.ofEpochMilli(0);

	TestClientAsync(int id) {
		this.cid = id;
		fileRand = new Random();
		reqRand = new Random();
		//clock = ApproximateClock.instance();
	}

	Result[] runTest(boolean isController, double reqRateMPS, int writeRatio, int totalClients, int clientsPerMachine, int testDurSec, int filesPerclient, int batchSize,
						   int warmupSecs, Record recGen, final LinkedBlockingQueue<Instant> rq, TestClientServiceClient rpcClient,
					 	double highMPS, int burstProb, int burstDurSec, boolean isSynchronous, boolean readRecent) throws KawkabException {

		int offset = (cid-1)*filesPerclient;
		if (writeRatio > 0)
			openFiles(offset, filesPerclient, recGen.size());
		else
			openFilesForReads(offset, filesPerclient, recGen.size());

		timestamps = new long[files.length];

		rpcClient.barrier(cid);

		boolean isBursty = burstProb > 0 && burstDurSec > 0;
		BurstGenerator bg = null;
		Thread ctrlThr = null;
		if (isController && !isSynchronous) {
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				e.printStackTrace();
				return null;
			}

			work = true;
			if (isBursty)
				ctrlThr = new Thread(() -> generateReqsBursty(reqRateMPS, highMPS, burstProb, burstDurSec, totalClients, clientsPerMachine, batchSize, rq));
			else
				ctrlThr = new Thread(() -> generateReqs(reqRateMPS, totalClients, clientsPerMachine, batchSize, rq));
			ctrlThr.start();
		}

		if (isBursty && isSynchronous) {
			bg = new BurstGenerator(reqRateMPS, highMPS, burstProb, burstDurSec, totalClients, clientsPerMachine, batchSize);
		}

		System.out.printf("Is synchronous = %s and bursty = %s\n", isSynchronous, isBursty);

		Result[] res = null;
		try {
			System.out.printf("Ramp-up for %d seconds..., %s \n", warmupSecs, new SimpleDateFormat("HH:mm:ss.SSS").format(new Date()));
			test(warmupSecs, recGen.newRecord(), batchSize, writeRatio, rq, false, isController, reqRateMPS, isSynchronous, isBursty, bg, readRecent);

			System.out.printf("Running test for %d seconds... %S\n", testDurSec, new SimpleDateFormat("HH:mm:ss.SSS").format(new Date()));
			res = test(testDurSec, recGen.newRecord(), batchSize, writeRatio, rq, true, isController, reqRateMPS, isSynchronous, isBursty, bg, readRecent);

			System.out.printf("Ramp-down for %d seconds... %S\n", rampDownSec, new SimpleDateFormat("HH:mm:ss.SSS").format(new Date()));
			test(rampDownSec, recGen.newRecord(), batchSize, writeRatio, rq, false, isController, reqRateMPS, isSynchronous, isBursty, bg, readRecent);
		}catch (AssertionError | Exception e) {
			e.printStackTrace();
		} finally {
			closeFiles();
		}

		System.out.println(cid+" closing...");

		//rpcClient.barrier(cid); //This is needed before closing the controller

		if (isController && !isSynchronous) {
			try {
				Thread.sleep(3000);
			} catch (InterruptedException e) {}
			work = false;
			ctrlThr.interrupt();
			try {
				ctrlThr.join(2000);
			} catch (InterruptedException e) {
				e.printStackTrace();
				ctrlThr.interrupt();
			}

			System.out.println("Generator signaled.");
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
			sleep(waitTimeMicros);

			int size = rq.size();
			if ((size % 100) == 1) {
				System.out.print(size +" ");
			}

			for (int i=0; i<toSend; i++) {
				rq.add(clock.instant());
			}
		}

		//To let anyone finish if waiting to receive an item
		for (int i=0; i<2*clientsPerMachine; i++) {
			rq.add(nullInstant);
		}
	}

	private void generateReqsBursty(final double lowMPS, final double highMPS, final int burstProb, final int burstDurSec,
									int totalClients, int clientsPerMachine, int batchSize, final LinkedBlockingQueue<Instant> rq) {



		//double targetMPS = reqRateMPS/((double)totalCleints/(double)clientsPerMachine)/(double)batchSize; //total / ratePerMachine / batchSize
		//double iatMicros = 1 / (targetMPS);  //Convert millions per second to nano second wait interval

		/*double iatMicrosLow = iatMicros(lowMPS, totalClients, clientsPerMachine, batchSize);
		double iatMicrosHigh = iatMicros(highMPS, totalClients, clientsPerMachine, batchSize);

		System.out.printf("iatMicros low = %f, iatMicros high = %f, burstProb=%d, burstDurSec=%d, tc=%d, cpm=%d, bs=%d, mpsLow=%f, mpsHigh=%f\n",
				iatMicrosLow, iatMicrosHigh, burstProb, burstDurSec, totalClients, clientsPerMachine, batchSize, lowMPS, highMPS);

		Random burstRand = new Random(1);
		PoissonDistribution lowRand = new PoissonDistribution(iatMicrosLow); //arrival time for the next request
		PoissonDistribution highRand = new PoissonDistribution(iatMicrosHigh); //arrival time for the next request

		int elapsedSec = 0;
		int lastSet = 0;
		int waitTimeMicros = lowRand.sample();
		ApproximateClock apClock = ApproximateClock.instance();
		long startT = apClock.currentTime();*/

		BurstGenerator bgen = new BurstGenerator(lowMPS, highMPS, burstProb, burstDurSec, totalClients, clientsPerMachine, batchSize);
		while(work) {
			/*elapsedSec = (int)((apClock.currentTime()-startT)/1000.0);
			if (elapsedSec > lastSet) {
				lastSet = elapsedSec;
				if (elapsedSec % burstDurSec == 0) {
					int prob = burstRand.nextInt(100);
					if (prob < burstProb)
						waitTimeMicros = lowRand.sample();
					else
						waitTimeMicros = highRand.sample();
				}
			}

			sleep(waitTimeMicros);*/

			bgen.sleepNext();

			int size = rq.size();
			if ((size % 100) == 1) {
				System.out.print(size +" ");
			}

			rq.add(clock.instant());
		}

		//To let anyone finish if waiting to receive an item
		for (int i=0; i<2*totalClients; i++) {
			rq.add(nullInstant);
		}

		System.out.println("Request generator closed...");
	}

	private double iatMicros(double rateMPS, int totalClients, int clientsPerMachine, int batchSize) {
		double targetMPS = rateMPS/((double)totalClients/(double)clientsPerMachine)/(double)batchSize; //total / ratePerMachine / batchSize
		double iatMicros = 1 / (targetMPS);  //Convert millions per second to nano second wait interval
		return iatMicros;
	}

	private void sleep(int waitTimeMicros) {
		if (waitTimeMicros > 60) { //It sleeps for at least 60us.
			LockSupport.parkNanos(waitTimeMicros*1000);
		} else {
			busyWaitMicros(waitTimeMicros);
		}
	}

	private void generateReqsSynchronous(int totalCleints, int clientsPerMachine,
							  int batchSize, final LinkedBlockingQueue<Instant> rq) {
		System.out.println("Generating requests synchronously");

		while(work) {
			try {
				rq.offer(clock.instant(), 100, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				break;
			}
		}

		//To let anyone finish if waiting to receive an item
		for (int i=0; i<2*clientsPerMachine; i++) {
			rq.add(nullInstant);
		}
	}


	private Result[] test(int durSec, Record recGen, final int reqBatchSize, final int writeRatio,
						  final LinkedBlockingQueue<Instant> rq, boolean logStats, boolean isController,
						  double repRateMPS, boolean isSynchronous, boolean isBursty, BurstGenerator bg,
						  boolean readRecent)
			throws OutOfMemoryException, KawkabException {
		AccumulatorMap wLats = new AccumulatorMap(1000000);
		AccumulatorMap rLats = new AccumulatorMap(1000000);
		Accumulator rOpsThr = new Accumulator(durSec);
		Accumulator wTputs = new Accumulator(durSec);
		Accumulator rRps = new Accumulator(durSec);
		Accumulator wRps = new Accumulator(durSec);

		String[] fnames = new String[reqBatchSize]; //Random files used in a batch. Actual file name is populated from files class variable.
		long[] histReadTS = new long[files.length];
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
		long now = System.currentTimeMillis();
		long et = now + durSec*1000;
		while((now = apClock.currentTime()) < et) {
			try {
				Instant ts = null;
				if (!isSynchronous) {
					//ts = rq.take();
					ts = rq.poll(5, TimeUnit.SECONDS);
					if (ts == null)
						continue;

					if (ts.equals(nullInstant)) {
						System.out.printf("[TCA] %d received stop signal\n", cid);
						break;
					}
				} else {
					if (isBursty) bg.sleepNext();
					ts = clock.instant();
				}

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
					if (readRecent)
						batchSize = sendFixedRandomWindowReadRequest(recGen, reqBatchSize, timestamps);
					else
						batchSize = sendHistoricalReadRequest(recGen, histReadTS, reqBatchSize);

					//batchSize = sendReadRequest(recGen, apClock.currentTime());
					//batchSize = sendFixedWindowReadRequest(recGen, reqBatchSize, timestamps);
					//batchSize = sendFixedRandomWindowReadRequest(recGen, reqBatchSize, timestamps);
					//batchSize = sendHistoricalReadRequest(recGen, histReadTS, reqBatchSize);
					//batchSize = sendHistoricalReadRandomWindowRequest(recGen, histReadTS, reqBatchSize);
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
				if (elapsed < durSec) {
					if (isAppend) {
						wLats.put(lat, 1);
						wTputs.put(elapsed, 1);
						wRps.put(elapsed, batchSize);
					} else {
						rLats.put(lat, 1);
						rOpsThr.put(elapsed, 1);
						rRps.put(elapsed, batchSize);
						rBatchSize += batchSize;
					}
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
			readRes = prepareResult(rLats, rOpsThr, rRps, recGen.size());
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
		if (winSizeRecords < batchSize)
			winSizeRecords = 10*batchSize;

		long minLimit = maxLimit - winSizeRecords;
		if (minLimit < 0) minLimit = 0;

		//System.out.printf("minTs=%d, maxTs=%d\n", minTs, maxTs);

		long minTS = minLimit + reqRand.nextInt(winSizeRecords);
		if (minTS + batchSize > maxLimit) minTS = maxLimit - batchSize;

		long maxTS = minTS + batchSize;

		return client.readRecordsCounts(fn, minTS, maxTS, recGen, true);
	}

	private int sendFixedWindowReadRequest(Record recGen, int batchSize, long[] timestamps) throws KawkabException {
		int iFile = fileRand.nextInt(files.length);
		String fn = files[iFile];
		long maxTs = timestamps[iFile];
		if (maxTs < batchSize)
			return 0;

		long minTs = maxTs - batchSize;

		//System.out.printf("minTs=%d, maxTs=%d\n", minTs, maxTs);

		return client.readRecordsCounts(fn, minTs, maxTs, recGen, true);
	}

	private int sendHistoricalReadRequest(Record recGen, long[] timestamps, int batchSize) throws KawkabException {
		int iFile = fileRand.nextInt(files.length);
		String fn = files[iFile];

		long minTS = timestamps[iFile]+1;
		long maxTS = minTS + batchSize;

		long fs = client.size(fn)/recGen.size();

		int numRead = client.readRecordsCounts(fn, minTS, maxTS, recGen, false);

		timestamps[iFile] += numRead;

		//System.out.printf("[f%d-m%d-x%d-r%d-fs%d]",iFile,minTS,maxTS,numRead,fs);

		return numRead;

		//List<Record> res = client.readRecords(fn, minTS, maxTS, recGen, false);
		//if (res == null)
		//	return 0;
		//return res.size();
	}

	private int sendHistoricalReadRandomWindowRequest(Record recGen, long[] timestamps, int batchSize) throws KawkabException {
		int iFile = fileRand.nextInt(files.length);
		String fn = files[iFile];

		long fs = client.size(fn);

		long maxTS = (long)(fs/100.0*(10+reqRand.nextInt(50)));
		long minTS = maxTS - batchSize;
		if (minTS < 0) minTS = 0;

		return client.readRecordsCounts(fn, minTS, maxTS, recGen, false);

		//List<Record> res = client.readRecords(fn, minTS, maxTS, recGen, false);
		//if (res == null)
		//	return 0;
		//return res.size();
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

	void openFilesForReads(int offset, int numFiles, int recSize) throws KawkabException {
		assert client.isConnected();
		assert files == null : "Files are already open";

		System.out.printf("Opening files for reads: offset=%d, nf=%d, cid=%d\n", offset, numFiles, cid);

		files = new String[numFiles];
		Filesystem.FileMode[] modes = new Filesystem.FileMode[numFiles];
		int[] recSizes = new int[numFiles];

		for (int i=0; i<files.length; i++) {
			files[i] = String.format("tf-%d",offset+i);
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

	private Result prepareResult(AccumulatorMap lats, Accumulator opTputs, Accumulator rps, int recSize) {
		long cnt = lats.count();
		//double sizeMB = recSize*cnt / (1024.0 * 1024.0);
		//double thr = sizeMB * 1000.0 / durMsec;
		double opThr = opTputs.countsMean(); //Stats.meanOf(tputs.sortedBucketVals());
		double recsPerSec = rps.countsMean(); //(int)Stats.meanOf(rps.sortedBucketVals());
		double dataThr = recsPerSec*recSize / (1024.0 * 1024.0);

		return new Result(cnt, opThr, dataThr, lats.min(), lats.max(), lats, rps, recsPerSec);
		//return new Result(cnt, opThr, thr, lats.min(), lats.max(), lats.buckets(), rps.buckets(), recsPerSec);
	}

	private void busyWaitMicros(long micros){
		long waitUntil = System.nanoTime() + (micros * 1_000);
		while(waitUntil > System.nanoTime()){ ; }
	}

	private class BurstGenerator {
		private int elapsedSec;
		private long startT;
		private int waitTimeMicros;
		private int burstEndSec;
		private boolean bursting;

		private final int burstDurSec;
		//private Random burstRand;
		private final int burstProb;
		//private final PoissonDistribution lowRand;
		//private final PoissonDistribution highRand;
		private final int iatMicrosLow;
		private final int iatMicrosHigh;
		private ApproximateClock apClock = ApproximateClock.instance();
		private boolean inited = false;

		private BurstGenerator(final double lowMPS, final double highMPS, final int burstProb, final int burstDurSec,
							   int totalClients, int clientsPerMachine, int batchSize) {
			this.burstDurSec = burstDurSec;
			this.burstProb = burstProb;

			iatMicrosLow = (int)(iatMicros(lowMPS, totalClients, clientsPerMachine, batchSize));
			iatMicrosHigh = (int)(iatMicros(highMPS, totalClients, clientsPerMachine, batchSize));
			//lowRand = new PoissonDistribution(iatMicrosLow); //arrival time for the next request
			//highRand = new PoissonDistribution(iatMicrosHigh); //arrival time for the next request

			System.out.printf("iatMicros low = %d, iatMicros high = %d, burstProb=%d, burstDurSec=%d, tc=%d, cpm=%d, bs=%d, mpsLow=%f, mpsHigh=%f\n",
					iatMicrosLow, iatMicrosHigh, burstProb, burstDurSec, totalClients, clientsPerMachine, batchSize, lowMPS, highMPS);

		}

		private void reset() {
			inited = true;
			//burstRand = new Random(1);
			elapsedSec = 0;
			waitTimeMicros = iatMicrosLow; //lowRand.sample();
			startT = apClock.currentTime();
			bursting = false;
			burstEndSec = burstDurSec;
		}

		private void sleepNext() {
			if (!inited)
				reset();

			elapsedSec = (int)((apClock.currentTime()-startT)/1000.0);
			boolean windowEnd = elapsedSec > burstEndSec;
			if (windowEnd) {
				if (bursting) {
					bursting = false;
					waitTimeMicros = iatMicrosLow;
					burstEndSec = elapsedSec + burstDurSec;
					System.out.printf("No burst window from %d to %d\n", elapsedSec, burstEndSec);
				} else {// if (burstRand.nextInt(100) < burstProb) {
					waitTimeMicros = iatMicrosHigh;
					bursting = true;
					burstEndSec = elapsedSec + burstDurSec;
					System.out.printf("Burst window from %d to %d\n", elapsedSec, burstEndSec);
				}
			}

			sleep(waitTimeMicros);
		}
	}
}
