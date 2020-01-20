package kawkab.fs.cli;

import com.google.common.base.Stopwatch;
import com.google.common.math.Quantiles;
import kawkab.fs.api.FileOptions;
import kawkab.fs.api.Record;
import kawkab.fs.client.KClient;
import kawkab.fs.commons.Configuration;
import kawkab.fs.commons.Stats;
import kawkab.fs.core.Cache;
import kawkab.fs.core.FileHandle;
import kawkab.fs.core.Filesystem;
import kawkab.fs.core.Filesystem.FileMode;
import kawkab.fs.core.exceptions.KawkabException;
import kawkab.fs.records.BytesRecord;
import kawkab.fs.records.SampleRecord;
import kawkab.fs.records.SixteenRecord;
import kawkab.fs.testclient.Result;
import kawkab.fs.utils.GCMonitor;
import kawkab.fs.utils.LatHistogram;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;

public final class CLI {
	private Filesystem fs;
	private Map<String, FileHandle> openedFiles;
	private KClient client;
	
	public CLI() {
		openedFiles = new HashMap<>();
	}
	
	public static void main(String[] args) throws IOException, KawkabException, InterruptedException {
		CLI c = new CLI();

		c.initFS();
		c.cmd();
		c.shutdown();
	}

	private void initFS() throws IOException, KawkabException, InterruptedException {
		//Constants.printConfig();
		int nodeID = Configuration.getNodeID();
		String propsFile = System.getProperty("conf", Configuration.propsFileCluster);
		
		System.out.println("Node ID = " + nodeID);
		System.out.println("Loading properties from: " + propsFile);
		
		Properties props = Configuration.getProperties(propsFile);
		fs = Filesystem.bootstrap(nodeID, props);
	}

	private void cmd() throws IOException {
		String cmds = "Commands: cc, co, ca, cr, cd, cf, open, read, rr, apnd, ar, at, size, stats, clear, flush, gc, exit";
		
		try (BufferedReader ir = new BufferedReader(new InputStreamReader(System.in))) {
			System.out.println("--------------------------------");
			System.out.println(cmds);
			System.out.println("--------------------------------");
			String line;
			boolean next = true;

			Thread.currentThread().setName("CLI-thread");
			
			while (next) {
				System.out.print("\n$>");
				System.out.flush();

				line = ir.readLine();
				if (line == null)
					break;

				try {
					line = line.trim();
					if (line.length() == 0)
						continue;

					String[] args = line.split(" ");

					String cmd = args[0];
					
					switch (cmd) {
						case "gc":
							parseGC(args);
							continue;
						case "stats":
							parseStats(args);
							continue;
						case "cc":
							parseClientConnect(args);
							continue;
						case "co":
							parseClientOpen(args);
							continue;
						case "ca":
							parseClientAppend(args);
							continue;
						case "cr":
							parseClientRead(args);
							continue;
						case "cd":
							parseClientDisconnect(args);
							continue;
						case "cf":
							pareseClientFlush();
							continue;
						case "ar":
							parseAppendRecord(args);
							continue;
						case "rr":
							parseReadRecord(args);
							continue;
						case "open":
							parseOpen(args);
							continue;
						case "read":
							parseRead(args);
							continue;
						case "apnd":
							parseAppend(args);
							continue;
						case "size":
							parseSize(args);
							continue;
						case "at":
							parseAppendTest(args);
							continue;
						case "flush":
							dropCache();
							continue;
						case "clear":
							clearStats();
							continue;
						case "exit":
							next = false;
							break;
						default:
							System.out.println(cmds);
					}
				} catch (Exception | AssertionError e) {
					e.printStackTrace();
				}
			}
			
			for (FileHandle file: openedFiles.values()) {
				try {
					fs.close(file);
				} catch (KawkabException e) {
					e.printStackTrace();
				}
			}
		}
	}

	private void clearStats() throws KawkabException {
		GCMonitor.resetStats();
		fs.resetStats();
		Cache.instance().resetStats();
	}

	private void pareseClientFlush() {
		if (client == null) {
			System.out.println("Client is not connected to any server. Connect the client using the command cc");
			return;
		}

		client.flush();
	}

	private void parseGC(String[] args) {
		String usage = "Usage: gc <e|d|p>\ne Print GC events\n d Print GC durations\n p Perform GC";
		if (args.length != 2) {
			System.out.println("Incorrect parameters: " + usage);
		}

		switch (args[1]) {
			case "e": {
				GCMonitor.printEvents();
				break;
			}
			case "d": {
				GCMonitor.printDurations();
			}
			case "p": {
				System.gc();
				break;
			}
			default:
				System.out.println("Incorrect parameters: " + usage);
		}
	}

	private void parseStats(String[] args) throws KawkabException {
		System.out.println("Current Time: " + Instant.now());
		System.out.printf("GC stats (ms): %s\n", GCMonitor.getStats());
		System.out.printf("Cache stats:\n%s", Cache.instance().getStats());
		System.out.println("File stats:");
		fs.printStats();
	}

	private void parseClientConnect(String[] args) {
		if (client != null) {
			System.out.printf("Client is already connected to %s:%d\n", client.ip(), client.port());
			return;
		}

		String usage = "Usage: cc <ip> <port>";

		if (args.length != 3) {
			System.out.println("Incorrect parameters: " + usage);
			return;
		}

		client = new KClient(1);
		try {
			client.connect(args[1], Integer.parseInt(args[2]));
		} catch (KawkabException e) {
			e.printStackTrace();
			client = null;
		}
	}

	private void parseClientDisconnect(String[] args) {
		if (client == null) {
			System.out.println("Client is not connected to any server. Connect the client using the command cc");
			return;
		}

		try {
			client.disconnect();
		} catch (KawkabException e) {
			e.printStackTrace();
		}

		client = null;

		System.out.println("The client is disconnected.");
	}

	private void parseClientOpen(String[] args) throws InterruptedException, IOException, KawkabException {
		if (client == null) {
			System.out.println("Client is not connected to any server. Connect the client using the command cc");
			return;
		}

		String usage = "Usage: co <filename> <r rec|a> [<recSize>]";

		if (args.length < 3) {
			System.out.println(usage);
			return;
		}

		String fn = args[1];
		String fm = args[2];
		int recSize = args.length == 4 ? Integer.parseInt(args[3]) : 50;
		FileMode mode = parseMode(fm);
		client.open(fn, mode, recSize);
	}

	private void parseClientAppend(String args[]) throws KawkabException {
		String usage = "Usage: ca <filename> [<num-recs>]";
		if (args.length < 2) {
			System.out.println(usage);
			return;
		}

		if (client == null) {
			System.out.println("Client is not connected to any server. Connect the client using the command cc");
			return;
		}

		String fname = args[1];
		int numRecs = args.length > 2 ? Integer.parseInt(args[2]) : 1;
		int recSize = client.recordSize(fname);
		Random rand = new Random();
		long tsOffset = client.size(fname)/recSize + 1;
		Record recgen = recGen(recSize);
		for (int i=0; i<numRecs; i++) {
			client.append(fname, recgen.newRandomRecord(rand, i+tsOffset));
		}

		System.out.printf("Appended %d records of %d bytes. New file size is %d records.\n",numRecs, recSize, client.size(fname)/recSize);
	}

	private void parseClientRead(String[] args) throws KawkabException, IOException, InterruptedException {
		String usage = "Usage: cr <filename> [ <n num> | <t ts> | <r ts1 ts2> [p] | " +
				"<rn|rt ts1 ts2 count> [f] [l] | <np [count]>  [l] | <rr tsMin tsMax tsIval count> [f] [l] ] ";
		if (args.length < 2) {
			System.out.println(usage);
			return;
		}

		if (client == null) {
			System.out.println("Client is not connected to any server. Connect the client using the command cc");
			return;
		}

		String fname = args[1];
		int recSize = client.recordSize(fname);
		Record recgen = recGen(recSize);

		if (args.length == 2) {
			List<Record> recs = client.readRecords(fname, 1, Long.MAX_VALUE-1, recgen, true);

			if (recs == null) {
				System.out.printf("Records not found b/w %d and %d\n", 1, Long.MAX_VALUE-1);
				return;
			}

			for (Record rec : recs) {
				System.out.println(rec);
			}
			return;
		}

		String type = args[2];
		switch (type) {
			case "n": {
				if (args.length < 4) {
					System.out.println("Insufficient args: " + usage);
					break;
				}

				int recNum = Integer.parseInt(args[3]);

				Stopwatch sw = Stopwatch.createStarted();
				recgen = client.recordNum(fname, recNum, recgen, true);
				long elapsed = sw.stop().elapsed(TimeUnit.MICROSECONDS);

				System.out.println(recgen);
				System.out.printf("Latency (ns): %,d\n", elapsed);
				break;
			}
			case "t": {
				if (args.length < 4) {
					System.out.println("Insufficient args: " + usage);
					break;
				}

				long atTS = Long.parseLong(args[3]);

				Stopwatch sw = Stopwatch.createStarted();
				recgen = client.recordAt(fname, atTS, recgen, true);
				long elapsed = sw.stop().elapsed(TimeUnit.MICROSECONDS);

				if (recgen != null)
					System.out.println(recgen);
				else
					System.out.println("Record not found at " + atTS);

				System.out.printf("Latency (ns): %,d\n", elapsed);

				break;
			}
			case "r": {
				if (args.length < 5) {
					System.out.println("Two timestamps are required: " + usage);
					break;
				}

				long t1 = Long.parseLong(args[3]);
				long t2 = Long.parseLong(args[4]);

				Stopwatch sw = Stopwatch.createStarted();
				List<Record> recs = client.readRecords(fname, t1, t2, recgen, true);
				long elapsed = sw.stop().elapsed(TimeUnit.MICROSECONDS);

				if (recs == null) {
					System.out.printf("Records not found b/w %d and %d\n", t1, t2);
					break;
				}

				if (args.length == 6) {
					for (Record rec : recs) {
						System.out.println(rec);
					}
				}

				System.out.println("Read records: " + recs.size());
				System.out.printf("Latency (ns): %,d\n", elapsed);

				break;
			}
			case "rn":
			case "rt": {
				if (args.length < 6) {
					System.out.println("Reads numRecs randomly selected b/w ts1 and ts2, needs four args\n" + usage);
					break;
				}

				long t1 = Long.parseLong(args[3]);
				long t2 = Long.parseLong(args[4]);
				int numRecs = Integer.parseInt(args[5]);

				boolean withFlush = false;
				boolean loadFromPrimary = false;

				switch(args.length) {
					case 8: {
						if (args[7].equals("l"))
							loadFromPrimary = true;
					}
					case 7: {
						if (args[6].equals("f"))
							withFlush = true;
						else if (args[6].equals("l"))
							loadFromPrimary = true;
					} break;
					case 6:
						withFlush = loadFromPrimary = false;
						break;
					default: {
						System.out.println(usage);
						return;
					}
				}

				clientExactReadTest(fname, recSize, t1, t2, numRecs, recgen, type.equals("rn"), withFlush, loadFromPrimary);

				break;
			}
			case "rr":{
				if (args.length < 7) {
					System.out.println("Range query requires five args\n" + usage);
					break;
				}

				long t1 = Long.parseLong(args[3]);
				long t2 = Long.parseLong(args[4]);
				int ival = Integer.parseInt(args[5]);
				int count = Integer.parseInt(args[6]);

				boolean withFlush = false;
				boolean loadFromPrimary = false;

				switch(args.length) {
					case 9: {
						if (args[8].equals("l"))
							loadFromPrimary = true;
					}
					case 8: {
						if (args[7].equals("f"))
							withFlush = true;
						else if (args[7].equals("l"))
							loadFromPrimary = true;
					} break;
					case 7:
						withFlush = loadFromPrimary = false;
						break;
					default: {
						System.out.println(usage);
						return;
					}
				}

				clientRangeQueryTest(fname, recSize, t1, t2, ival, count, recgen, withFlush, loadFromPrimary);

				break;
			}
			case "np": {
				int count = args.length == 4 ? Integer.parseInt(args[3]) : 1000000;
				readNoops(fname, count);
				break;
			}
			default:
				System.out.printf("Invalid read type %s. %s\n", type, usage);
		}
	}

	private void readNoops(String fname, int count) throws KawkabException {
		int recSize = client.recordSize(fname);

		LatHistogram tlog = new LatHistogram(TimeUnit.MICROSECONDS, "NoOPRead latency", 100, 1000);
		System.out.printf("Reading %,d noops, recSize %d\n", count, recSize);

		long startTime = System.currentTimeMillis();
		for (int i=0; i<count; i++) {
			tlog.start();
			client.noopRead(recSize);
			tlog.end();
		}

		assert count == tlog.sampled();

		double durSec = (System.currentTimeMillis() - startTime) / 1000.0;

		Result res = getResults(tlog, durSec, count, recSize, 1, "NoOP reads");
		System.out.println(res.csvHeader());
		System.out.println(res.csv());
	}

	private void clientRangeQueryTest(String fname, int recSize, long t1, long t2, int interval, int count, Record recgen,
									  boolean withFlush, boolean loadFromPrimary) throws KawkabException, IOException, InterruptedException {
		Random rand = new Random();
		TimeUnit unit = TimeUnit.MICROSECONDS;
		LatHistogram latHist = new LatHistogram(unit, "Range query latency", 100, 1000000);

		System.out.printf("Reading records %,d times from %s randomly b/w %d and %d with interval %d, recSize %d, flush is %s, loadFromPrimary is %s\n",
				count, fname, t1, t2, interval, recSize, withFlush, loadFromPrimary);

		long diff = t2 - t1;
		long[] tsMin = new long[count];
		long[] tsMax = new long[count];
		int tries = 100;
		for (int i=0; i<count && tries>0; i++) {
			long minVal = Math.abs(rand.nextLong() % diff);
			if (diff > 0 ) {
				if (t1 + minVal + interval - 1 > t2) {
					i--;
					tries--;
					continue;
				}
				tries = 100;

				tsMin[i] = t1 + minVal;
				tsMax[i] = tsMin[i] + interval - 1;
			} else {
				tsMin[i] = t1;
				tsMax[i] = t2;
			}
		}

		long dur = 0;
		int numRecs = 0;
		long[] lats = new long[count];
		for (int i=0; i<count; i++) {
			assert tsMin[i] >= 0;
			assert tsMax[i] >= 0;

			if (withFlush ) {
				client.flush();
			}

			latHist.start();
			List<Record> recs = client.readRecords(fname, tsMin[i], tsMax[i], recgen, loadFromPrimary);
			long lat = latHist.end();

			if (lat < 0) {
				i--;
				continue;
			}

			dur += lat;
			lats[i] = lat;

			//To ensure that we have same number of results for the correct results
			assert recs.size() == interval : String.format("Expected %d results, got %d", interval, recs.size());

			numRecs += recs.size();
		}

		double durSec = unit.toSeconds(dur);

		Result res = getResults(latHist, durSec, numRecs, recSize, interval, "Range query test");
		System.out.println(res.csvHeader());
		System.out.println(res.csv());

		printResults(lats, recSize, interval, durSec);
	}

	private void clientExactReadTest(String fname, int recSize, long t1, long t2, int numRecs, Record recgen,
									 boolean byRecNum, boolean withFlush, boolean loadFromPrimary) throws KawkabException {
		Random rand = new Random();

		TimeUnit unit = TimeUnit.MICROSECONDS;
		LatHistogram tlog = new LatHistogram(unit, "Read latency", 100, 1000000);

		System.out.printf("Reading %,d records from %s using %s, recSize %d, in range %d and %d\n", numRecs, fname,
				byRecNum?"rec nums":"exact time", recSize, t1, t2);

		long dur = 0;

		long[] lats = new long[numRecs];

		for (int i=0; i<numRecs; i++) {
			if (withFlush) {
				client.flush();
			}

			long lat;
			if (byRecNum) {
				int recNum = (int) (t1 + rand.nextInt((int) (t2 - t1)));
				tlog.start();
				recgen = client.recordNum(fname, recNum, recgen, loadFromPrimary);
				lat = tlog.end();

			} else {
				int recTime = (int) (t1 + rand.nextInt((int) (t2 - t1)));
				tlog.start();
				recgen = client.recordAt(fname, recTime, recgen, loadFromPrimary);
				lat = tlog.end();
			}

			if (lat < 0) {
				i--;
				continue;
			}

			dur += lat;
			lats[i] = lat;
		}

		assert numRecs == tlog.sampled();

		double durSec = unit.toSeconds(dur);

		Result res = getResults(tlog, durSec, numRecs, recSize, 1, "Read test");
		System.out.println(res.csvHeader());
		System.out.println(res.csv());

		printResults(lats, recSize, 1, durSec);
	}

	private void printResults(long[] lats, int recSize, int batchSize, double durSec) {
		com.google.common.math.Stats stats = com.google.common.math.Stats.of(lats);
		Map<Integer, Double> quantiles =
				Quantiles.scale(100).indexes(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
						21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
						41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60,
						61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80,
						81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100).compute(lats);

		long cnt = stats.count()*batchSize;
		double sizeMB = recSize*cnt / (1024.0 * 1024.0);
		double thr = sizeMB / durSec;
		double opThr = cnt / durSec;

		System.out.printf("recSize=%d, numRecs=%d, opsTput=%,.0f OPS, dTput=%,.2f MB/s, mean=%.2f, " +
						"50%%=%.2f, 95%%=%.2f, 99%%=%.2f, min=%.2f, max=%.2f, batchSize=%d, stdev=%.2f\n",
				recSize, cnt, opThr, thr, stats.mean(), quantiles.get(50), quantiles.get(95), quantiles.get(99),
				stats.min(), stats.max(), batchSize, stats.sampleStandardDeviation()
				);

		System.out.printf("%d, %d, %.0f, %.2f, %.0f, %.2f, %.2f, %.2f, %.2f, %.2f, %d, %.2f\n",
				recSize, cnt, opThr, thr, stats.mean(), quantiles.get(50), quantiles.get(95), quantiles.get(99),
				stats.min(), stats.max(), batchSize, stats.sampleStandardDeviation()
		);

		System.out.println("\nCDF %%ile:");
		for(int i=1; i<=100; i++) {
			System.out.print(i+", ");
		}
		System.out.println("\nCDF vals:\n");
		for(int i=1; i<=100; i++) {
			System.out.print(quantiles.get(i)+", ");
		}
		System.out.println();
	}

	private Result getResults(LatHistogram latHist, double durSec, int numRecs, int recSize, int batchSize, String tag) {
		long cnt = latHist.sampled()*batchSize;
		double sizeMB = recSize*cnt / (1024.0 * 1024.0);
		double thr = sizeMB / durSec;
		double opThr = cnt / durSec;

		System.out.printf("%s: recSize=%d, numRecs=%d, rTput=%,.0f MB/s, opsTput=%,.0f OPS, Lat %s\n",
				tag, recSize, cnt, thr, opThr, latHist.getStats());

		return new Result(latHist.sampled(), opThr, thr, latHist.min(), latHist.max(), latHist.accumulator().histogram(), null, batchSize);
	}

	private void parseReadRecord(String[] args) throws IOException, KawkabException {
		String usage = "Usage: rr <filename> [<n num> | <t ts> | <r ts1 ts2>]";
		if (args.length < 2) {
			System.out.println(usage);
			return;
		}

		String fname = args[1];
		FileHandle file = openedFiles.get(fname);
		if (file == null) {
			throw new IOException("File not opened: " + fname);
		}

		int recSize = file.recordSize();
		Record recgen = recGen(recSize);

		if (args.length == 2) {
			List<Record> recs = file.readRecords(1, Long.MAX_VALUE-1, recgen.newRecord(), true);

			if (recs == null) {
				System.out.printf("Records not found b/w %d and %d\n", 1, Long.MAX_VALUE-1);
				return;
			}

			for (Record rec : recs) {
				System.out.println(rec);
			}
			return;
		}

		String type = args[2];
		switch (type) {
			case "n": {
				if (args.length < 4) {
					System.out.println("Insufficient args: " + usage);
					break;
				}

				int recNum = Integer.parseInt(args[3]);

				LatHistogram tlog = new LatHistogram(TimeUnit.MICROSECONDS, "read-lat", 100, 100000);
				tlog.start();
				file.recordNum(recgen.copyInDstBuffer(), recNum, recgen.size(), true);
				tlog.end();

				System.out.println(recgen);
				tlog.printStats();
				break;
			}
			case "t": {
				if (args.length < 4) {
					System.out.println("Insufficient args: " + usage);
					break;
				}

				long atTS = Long.parseLong(args[3]);
				if (file.recordAt(recgen.copyInDstBuffer(), atTS, recgen.size(), true))
					System.out.println(recgen);
				else
					System.out.println("Record not found at " + atTS);
				break;
			}
			case "r": {
				if (args.length < 5) {
					System.out.println("Two timestamps are required: " + usage);
					break;
				}

				long t1 = Long.parseLong(args[3]);
				long t2 = Long.parseLong(args[4]);
				List<Record> recs = file.readRecords(t1, t2, recgen, true);

				if (recs == null) {
					System.out.printf("Records not found b/w %d and %d\n", t1, t2);
					break;
				}

				for (Record rec : recs) {
					System.out.println(rec);
				}
				break;
			}
			default:
				System.out.printf("Invalid read type %s. %s\n", type, usage);
		}
	}

	private void parseAppendRecord(String[] args) throws IOException, KawkabException, InterruptedException {
		String usage = "Usage: ar <filename> [<numRecs>] ";
		if (args.length < 2) {
			System.out.println(usage);
			return;
		}

		String fname = args[1];
		int numRecs = args.length > 2 ? Integer.parseInt(args[2]) : 1;
		FileHandle file = openedFiles.get(fname);
		if (file == null) {
			throw new IOException("File not opened: " + fname);
		}

		Random rand = new Random();
		int recSize = file.recordSize();
		long tsOffset = file.size()/recSize + 1;

		Record recgen = recGen(recSize);
		Stopwatch sw = Stopwatch.createStarted();
		for (int i=0; i<numRecs; i++) {
			Record rec = recgen.newRandomRecord(rand, i+tsOffset);
			file.append(rec.copyOutSrcBuffer(), rec.size());
		}
		long elapsed = sw.stop().elapsed(TimeUnit.MICROSECONDS);

		System.out.printf("Appended %d records of %d bytes. New file size is %d records.\n",numRecs, recSize, file.size()/recSize);

		System.out.printf("Elapsed time (us): %,d\n",elapsed);
	}

	private void dropCache() throws KawkabException, IOException, InterruptedException {
		fs.flush();
		clearOSCache();
		//clearDiskCache();
	}

	private void clearOSCache() throws InterruptedException, IOException {
		/*Runtime run = Runtime.getRuntime(); // get OS Runtime
		// execute a system command and give back the process
		Process pr = run.exec("sudo sh -c \"echo 3 > /proc/sys/vm/drop_caches\"");
		pr.waitFor();*/

		String[] cmd = new String[]{"/bin/bash", "/home/sm3rizvi/kawkab/dropcaches.sh"};
		Process pr = Runtime.getRuntime().exec(cmd);
		int exitVal = pr.waitFor();
		if (exitVal != 0) {
			System.out.println("Failed to drop OS caches");
		}
	}

	private void clearDiskCache() throws IOException, InterruptedException {
		String fn = "/hdd1/sm3rizvi/kawkab/tempfile.img"; //A random 1GB file create using "fallocate -l 1G tempfile.img"

		try (
				RandomAccessFile file = new RandomAccessFile(fn, "r");
				SeekableByteChannel channel = file.getChannel()
		) {

			channel.position(0);
			int read = 1;
			ByteBuffer buf = ByteBuffer.allocate(16*1024*1024);
			while (read > 0) {
				buf.clear();
				read = channel.read(buf);
			}
			//assert bytesLoaded == 0;
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private void parseSize(String[] args) throws IOException, KawkabException {
		String usage = "Usage: size <filename>";
		if (args.length < 2) {
			System.out.println(usage);
			return;
		}
		
		String fname = args[1];
		FileHandle file = openedFiles.get(fname);
		if (file == null) {
			throw new IOException("File not opened: " + fname);
		}

		long fs = file.size();
		long fsMib = fs / 2048576;

		System.out.printf("File size = %d MiB, %d B, recsInFile=%d\n", fsMib, fs, file.recordsInFile());
	}
	
	private void parseAppendTest(String[] args) throws KawkabException, IOException {
		String usage = "Usage: at [<num writers> <req size> <data size MB>]";
		if (args.length != 1 && args.length != 4) {
			System.out.println(usage);
			return;
		}
		
		int reqSize = 100;
		int numWriters = 1;
		long dataSizeBytes = 9999L * 1048576;
		
		if (args.length == 4) {
			numWriters = Integer.parseInt(args[1]);
			reqSize = Integer.parseInt(args[2]);
			dataSizeBytes = Long.parseLong(args[3]) * 1048576;
		}
		
		System.out.printf("%d, %d, %d\n", reqSize, numWriters, dataSizeBytes);
		
		appendTest(reqSize, numWriters, dataSizeBytes);
	}
	
	private void appendTest(final int bufSize, final int numWriters, final long dataSize)
			throws IOException, KawkabException {
		System.out.println("----------------------------------------------------------------");
		System.out.println("            Append Performance Test - Concurrent Files");
		System.out.println("----------------------------------------------------------------");
		
		System.out.println("Current cache size; " + Cache.instance().size());
		
		final Filesystem fs = Filesystem.instance();
		
		Thread[] workers = new Thread[numWriters];
		
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
						
						LatHistogram tlog = new LatHistogram(TimeUnit.MICROSECONDS, "Main append", 5, 100000);
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
						
						System.out.printf("Writer %d: buffer=%dB, dataSize=%.0fMB, writeTput=%,.0f MB/s, opsTput=%,.0f OPS\n", id, bufSize, sizeMB, thr, opThr);
						tlog.printStats();
						tlog.reset();
						
						//Inode.tlog1.printStats();
						//Inode.tlog1.reset();
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
	
	private void parseAppend(String[] args) throws IOException, KawkabException, InterruptedException {
		String usage = "Usage: apnd <filename> [str <string> | file <filepath> | bytes <numBytes> <reqSize>]";
		if (args.length < 4) {
			System.out.println(usage);
			return;
		}

		String fname = args[1];
		String cmd = args[2];
		switch (cmd) {
			case "str": {
				StringBuilder sb = new StringBuilder().append(args[3]);
				for (int i = 4; i < args.length; i++) {
					sb.append(' ').append(args[i]);
				}
				byte[] data = sb.toString().getBytes();
				appendFile(fname, data, data.length, data.length);
				break;
			}
			case "bytes": {
				if (args.length < 5) {
					System.out.println(usage);
					return;
				}
				long dataSize;
				int bufLen;
				try {
					dataSize = Long.parseLong(args[3]);
					bufLen = Integer.parseInt(args[4]);
				} catch (NumberFormatException e) {
					System.out.println("Given invalid bytes numBytes or reqSize. " + usage);
					return;
				}
				
				byte[] data = randomData(bufLen);
				
				appendFile(fname, data, dataSize, bufLen);
				break;
			}
			case "file":
				appendFile(fname, args[3]);
				break;
			default:
				System.out.println(usage);
				break;
		}
	}
	
	private void appendFile(String fn, String srcFile) throws IOException,
			KawkabException, InterruptedException {
		try (DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(new File(srcFile))))) {
			int bufLen = 4*1024;
			byte[] buf = new byte[bufLen];
			int dataSize;
			while ((dataSize = dis.read(buf)) != -1) {
				appendFile(fn, buf, dataSize, buf.length);
			}
		}
	}
	
	private void appendFile(String fn, byte[] data, long dataSize, int bufLen) throws IOException,
			KawkabException, InterruptedException {
		FileHandle file = openedFiles.get(fn);
		if (file == null) {
			throw new IOException("File not opened: " + fn);
		}
		
		int sent = 0;
		while(sent < dataSize) {
			int toSend = (int)(dataSize-sent > bufLen ? bufLen : dataSize - sent);
			sent += file.append(data, 0, toSend);
		}
	}

	private void parseRead(String[] args)
			throws NumberFormatException, IOException, KawkabException {
		String usage = "Usage: read <filename> [<offset> [<numBytes> [<dstFile>]]]";
		
		String fn;
		String dstFile = null;
		long offset = 0;
		long len = 0;
		
		switch (args.length) {
		case 5:
			dstFile = args[4];
		case 4:
			try {
				len = Long.parseLong(args[3]);
			} catch (NumberFormatException e) {
				System.out.print("Given invalid length. " + usage);
				return;
			}
		case 3:
			try{
				offset = Long.parseLong(args[2]);
			} catch (NumberFormatException e) {
				System.out.print("Given invalid offset. Usage: " + usage);
				return;
			}
		case 2:
			fn  = args[1];
			break;
		default:
			System.out.println(usage);
			return;
		}
		
		readFile(fn, offset, len, dstFile, true);
		
		
		/*if (args.length > 4) {
			dstFile = args[4];
			len = Long.parseLong(args[3]);
			offset = Long.parseLong(args[2]);
			
		} else if (args.length > 3) {
			len = Long.parseLong(args[3]);
			offset = Long.parseLong(args[2]);
		} else if (args.length < 2) {
			System.out.println("Usage: read filename offset intLength <dstFile>");
			return;
		}
		readFile(args[1], offset, len, dstFile);
		*/

		
	}

	private void readFile(String fn, long byteStart, long readLen, String dst, boolean loadFromPrimary)
			throws IOException, KawkabException {
		FileHandle file = openedFiles.get(fn);
		if (file == null) {
			throw new IOException("File not opened: " + fn);
		}
		
		int bufLen = 16*1024*1024;
		byte[] buf = new byte[bufLen];
		long read = 0;
		BufferedWriter out = null;
		try {
			if (dst != null) {
				out = new BufferedWriter(new FileWriter(new File(dst)));
			}

			if (readLen <= 0) {
				readLen = file.size();
			}

			System.out.println("[CLI] read len bytes: " + readLen);
			while (read < readLen) {
				int toRead = (int) (readLen - read >= buf.length ? buf.length : readLen - read);
				int len = file.read(buf, byteStart+read, toRead, loadFromPrimary);
				System.out.println("Read length = " + len);
				if (out != null) {
					out.write(new String(buf, 0, len));
				} else {
					System.out.println(new String(buf, 0, len));
				}
				read += len;
			}
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} finally {
			if (out != null)
				out.close();
		}
		System.out.println();
	}

	private void parseOpen(String[] args) throws IOException, KawkabException, InterruptedException {
		if (args.length < 4) {
			System.out.println("Usage: open <filename> <r|a> <b|s> [<recSize>]");
			return;
		}

		String fn = args[1];
		String mode = args[2];
		String type = args[3];
		int recSize = args.length == 5 ? Integer.parseInt(args[4]) : 50;
		FileHandle fh = openFile(fn, mode, type, recSize);
		openedFiles.put(fn, fh);
	}

	private FileHandle openFile(String fn, String fm, String type, int recSize) throws IOException, KawkabException, InterruptedException {
		FileMode mode = parseMode(fm);

		FileOptions opts = parseFileOpts(type, recSize);

		return fs.open(fn, mode, opts);
	}
	
	private void shutdown() throws KawkabException, InterruptedException {
		fs.shutdown();
		System.out.println("Closed CLI");
	}


	private byte[] randomData(int length) {
		String chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
		int charLen = chars.length();
		Random rand = new Random();
		byte[] data = new byte[length];

		for (int i=0; i<data.length; i++) {
			data[i] = (byte)chars.charAt(rand.nextInt(charLen));
		}

		return data;
	}

	private FileMode parseMode(String fm) throws IOException {
		FileMode mode;
		if (fm.equals("r"))
			mode = FileMode.READ;
		else if (fm.equals("a"))
			mode = FileMode.APPEND;
		else
			throw new IOException("Invalid file mode");

		return mode;
	}

	private FileOptions parseFileOpts(String type, int recSize) throws KawkabException {
		FileOptions opts = new FileOptions();
		if (type.equals("s"))
			opts = new FileOptions(recSize);
		else if (!type.equals("b")) {
			throw new KawkabException("Invalid file type " + type);
		}
		return opts;
	}

	private Record recGen(int recSize) {
		switch(recSize) {
			case 16: {
				return new SixteenRecord();
			}
			case 50: {
				return new SampleRecord();
			}
			default: {
				return new BytesRecord(recSize);
			}
		}
	}
}
