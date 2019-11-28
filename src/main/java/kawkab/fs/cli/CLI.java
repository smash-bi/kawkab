package kawkab.fs.cli;

import com.google.common.base.Stopwatch;
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
import kawkab.fs.utils.TimeLog;

import java.io.*;
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
		String cmds = "Commands: cc, co, ca, cr, cd, cf, open, read, rr, apnd, ar, at, size, stats, flush, gc, exit";
		
		try (BufferedReader ir = new BufferedReader(new InputStreamReader(System.in))) {
			System.out.println("--------------------------------");
			System.out.println(cmds);
			System.out.println("--------------------------------");
			String line;
			boolean next = true;
			
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
							flushCache();
							continue;
						case "exit":
							next = false;
							break;
						default:
							System.out.println(cmds);
					}
				} catch (Exception e) {
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

	private void pareseClientFlush() {
		if (client == null) {
			System.out.println("Client is not connected to any server. Connect the client using the command cc");
			return;
		}

		client.flush();
	}

	private void parseGC(String[] args) {
		System.gc();
	}

	private void parseStats(String[] args) throws KawkabException {
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
		int recSize = args.length == 4 ? Integer.parseInt(args[3]) : 42;
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

	private void parseClientRead(String[] args) throws KawkabException {
		String usage = "Usage: cr <filename> [<n num> | <t ts> | <r ts1 ts2> [p] | <rn|rt ts1 ts2 numrecs [f]> | <np [count]>] ";
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
			List<Record> recs = client.readRecords(fname, 1, Long.MAX_VALUE-1, recgen);

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
				recgen = client.recordNum(fname, recNum, recgen);
				long elapsed = sw.stop().elapsed(TimeUnit.NANOSECONDS);

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
				recgen = client.recordAt(fname, atTS, recgen);
				long elapsed = sw.stop().elapsed(TimeUnit.NANOSECONDS);

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
				List<Record> recs = client.readRecords(fname, t1, t2, recgen);
				long elapsed = sw.stop().elapsed(TimeUnit.NANOSECONDS);

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

				clientExactReadTest(fname, recSize, t1, t2, numRecs, recgen, type.equals("rn"), args.length == 7);

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

		TimeLog tlog = new TimeLog(TimeUnit.NANOSECONDS, "NoOPRead latency", 100);
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

	private void clientExactReadTest(String fname, int recSize, long t1, long t2, int numRecs, Record recgen,
									 boolean byRecNum, boolean withFlush) throws KawkabException {
		Random rand = new Random();

		TimeLog tlog = new TimeLog(TimeUnit.NANOSECONDS, "Read latency", 100);

		System.out.printf("Reading %,d records from %s using %s, recSize %d, in range %d and %d\n", numRecs, fname,
				byRecNum?"rec nums":"exact time", recSize, t1, t2);

		long startTime = System.currentTimeMillis();

		for (int i=0; i<numRecs; i++) {
			tlog.start();
			if (byRecNum) {
				int recNum = (int) (t1 + rand.nextInt((int) (t2 - t1)));
				recgen = client.recordNum(fname, recNum, recgen);
			} else {
				int recTime = (int) (t1 + rand.nextInt((int) (t2 - t1)));
				recgen = client.recordAt(fname, recTime, recgen);
			}
			tlog.end();

			if (withFlush) {
				client.flush();
			}
		}

		assert numRecs == tlog.sampled();

		double durSec = (System.currentTimeMillis() - startTime) / 1000.0;

		Result res = getResults(tlog, durSec, numRecs, recSize, 1, "Read test");
		System.out.println(res.csvHeader());
		System.out.println(res.csv());
	}

	private Result getResults(TimeLog tlog, double durSec, int numRecs, int recSize, int batchSize, String tag) {
		long cnt = tlog.sampled()*batchSize;
		double sizeMB = recSize*cnt / (1024.0 * 1024.0);
		double thr = sizeMB / durSec;
		int opThr = (int)(cnt / durSec);

		System.out.printf("%s: recSize=%d, numRecs=%d, rTput=%,.0f MB/s, opsTput=%,d OPS, Lat %s\n",
				tag, recSize, numRecs, thr, opThr, tlog.getStats());

		double[] lats = tlog.stats();
		return new Result(tlog.sampled(), (int)opThr, thr, lats[0], lats[1], lats[2], tlog.min(), tlog.max(), tlog.mean(),
				lats[0], 0, tlog.mean(), 0, tlog.max(), 0, new long[]{}, new long[]{});
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
			List<Record> recs = file.readRecords(1, Long.MAX_VALUE-1, recgen.newRecord());

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

				TimeLog tlog = new TimeLog(TimeUnit.NANOSECONDS, "read-lat", 100);
				tlog.start();
				file.recordNum(recgen.copyInDstBuffer(), recNum, recgen.size());
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
				if (file.recordAt(recgen.copyInDstBuffer(), atTS, recgen.size()))
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
				List<Record> recs = file.readRecords(t1, t2, recgen);

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

		Stopwatch sw = Stopwatch.createStarted();
		Record recgen = recGen(recSize);
		for (int i=0; i<numRecs; i++) {
			Record rec = recgen.newRandomRecord(rand, i+tsOffset);
			file.append(rec.copyOutSrcBuffer(), rec.size());
		}
		long elapsed = sw.stop().elapsed(TimeUnit.NANOSECONDS);

		System.out.printf("Appended %d records of %d bytes. New file size is %d records.\n",numRecs, recSize, file.size()/recSize);

		System.out.printf("Elapsed time (ns): %,d\n",elapsed);
	}

	private void flushCache() throws KawkabException {
		fs.flush();
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
		System.out.printf("File size = %d MiB, %d B", fsMib, fs);
	}
	
	private void parseAppendTest(String[] args) throws KawkabException, IOException {
		String usage = "Usage: apndTest [<num writers> <req size> <data size MB>]";
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
						
						TimeLog tlog = new TimeLog(TimeUnit.NANOSECONDS, "Main append", 5);
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
		String usage = "Usage: read filename offset intLength <dstFile>";
		
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
		
		readFile(fn, offset, len, dstFile);
		
		
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

	private void readFile(String fn, long byteStart, long readLen, String dst)
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
				int len = file.read(buf, byteStart+read, toRead);
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
		int recSize = args.length == 5 ? Integer.parseInt(args[4]) : 42;
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
			case 42: {
				return new SampleRecord();
			}
			default: {
				return new BytesRecord(recSize);
			}
		}
	}
}
