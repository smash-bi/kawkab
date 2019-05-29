package kawkab.fs.cli;

import kawkab.fs.api.FileOptions;
import kawkab.fs.commons.Configuration;
import kawkab.fs.commons.Stats;
import kawkab.fs.core.Cache;
import kawkab.fs.core.FileHandle;
import kawkab.fs.core.Filesystem;
import kawkab.fs.core.Filesystem.FileMode;
import kawkab.fs.core.exceptions.KawkabException;
import kawkab.fs.utils.TimeLog;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

public final class CLI {
	private Filesystem fs;
	private Map<String, FileHandle> openedFiles;
	
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
		String cmds = "Commands: open, read, apnd, size, apndTest|at, flush, exit";
		
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
						case "apndTest":
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
							continue;
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
	
	private void flushCache() throws KawkabException {
		Cache.instance().flush();
	}
	
	private void parseSize(String[] args) throws IOException, KawkabException {
		String usage = "Usage: size <filename> ";
		if (args.length < 2) {
			System.out.println(usage);
			return;
		}
		
		String fname = args[1];
		FileHandle file = openedFiles.get(fname);
		if (file == null) {
			throw new IOException("File not opened: " + fname);
		}
		
		long sizeMiB = file.size()/2048576;
		System.out.println("File size = " + sizeMiB + " MiB");
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
						
						TimeLog tlog = new TimeLog(TimeLog.TimeLogUnit.NANOS, "Main append");
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
		if (args.length < 3) {
			System.out.println("Usage: open <filename> <r|a>");
			return;
		}

		String fn = args[1];
		String mode = args[2];
		FileHandle fh = openFile(fn, mode);
		openedFiles.put(fn, fh);
	}

	private FileHandle openFile(String fn, String fm) throws IOException, KawkabException, InterruptedException {
		FileMode mode;
		if (fm.equals("r"))
			mode = FileMode.READ;
		else if (fm.equals("a"))
			mode = FileMode.APPEND;
		else
			throw new IOException("Invalid file mode");

		return fs.open(fn, mode, new FileOptions());
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
}
