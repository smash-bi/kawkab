package kawkab.fs.cli;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import kawkab.fs.api.FileOptions;
import kawkab.fs.core.FileHandle;
import kawkab.fs.core.Filesystem;
import kawkab.fs.core.Filesystem.FileMode;
import kawkab.fs.core.exceptions.AlreadyConfiguredException;
import kawkab.fs.core.exceptions.FileAlreadyOpenedException;
import kawkab.fs.core.exceptions.IbmapsFullException;
import kawkab.fs.core.exceptions.InvalidFileOffsetException;
import kawkab.fs.core.exceptions.KawkabException;
import kawkab.fs.core.exceptions.MaxFileSizeExceededException;
import kawkab.fs.core.exceptions.OutOfMemoryException;

public final class CLI {
	private Filesystem fs;
	private Map<String, FileHandle> openedFiles;
	
	public CLI() {
		openedFiles = new HashMap<String, FileHandle>();
	}
	
	public static void main(String[] args) throws IOException, KawkabException, InterruptedException, AlreadyConfiguredException {
		CLI c = new CLI();

		c.initFS();
		c.cmd();
		c.shutdown();
	}

	private void initFS() throws IOException, KawkabException, InterruptedException, AlreadyConfiguredException {
		//Constants.printConfig();
		int nodeID = getNodeID();
		
		System.out.println("Node ID = " + nodeID);
		
		Properties props = getProperties();
		
		fs = Filesystem.bootstrap(nodeID, props);
	}

	private void cmd() throws IOException {
		String cmds = "Commands: open, read, apnd, size, exit";
		
		try (BufferedReader ir = new BufferedReader(new InputStreamReader(System.in));) {
			System.out.println("--------------------------------");
			System.out.println(cmds);
			System.out.println("--------------------------------");
			String line;

			while (true) {
				System.out.print("\n$>");
				System.out.flush();

				line = ir.readLine();
				if (line == null)
					break;

				try {
					line = line.trim();
					if (line.length() == 0)
						continue;

					String args[] = line.split(" ");

					String cmd = args[0];

					if (cmd.equals("open")) {
						parseOpen(args);
					} else if (cmd.equals("read")) {
						parseRead(args);
					} else if (cmd.equals("apnd")) {
						parseAppend(args);
					} else if (cmd.equals("size")) {
						parseSize(args);
					} else if (cmd.equals("exit")) {
						break;
					} else {
						System.out.println(cmds);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	private void parseSize(String[] args) throws IOException, OutOfMemoryException, MaxFileSizeExceededException,
	InvalidFileOffsetException, KawkabException, InterruptedException {
		String usage = "Usage: size <filename> ";
		if (args.length < 2) {
			System.out.println(usage);
			return;
		}
		
		String fname = args[1];
		FileHandle file = null;
		file = openedFiles.get(fname);
		if (file == null) {
			throw new IOException("File not opened: " + fname);
		}
		
		long size = file.size();
		System.out.println("File size = " + size + " bytes");
	}

	private void parseAppend(String[] args) throws IOException, OutOfMemoryException, MaxFileSizeExceededException,
			InvalidFileOffsetException, KawkabException, InterruptedException {
		String usage = "Usage: apnd <filename> <str <string>> | file <filepath> | bytes <numBytes> <reqSize>";
		if (args.length < 4) {
			System.out.println(usage);
			return;
		}

		String fname = args[1];
		String cmd = args[2];
		if (cmd.equals("str")) {
			StringBuilder sb = new StringBuilder().append(args[3]);
			for (int i=4; i<args.length; i++) {
				sb.append(' ').append(args[i]);
			}
			byte[] data = sb.toString().getBytes();
			appendFile(fname, data, 0, data.length);
		} else if (cmd.equals("bytes")) {
			if (args.length < 5) {
				System.out.println(usage);
				return;
			}
			long dataSize = -1;
			int bufLen = 0;
			try {
				dataSize = Long.parseLong(args[3]);
				bufLen = Integer.parseInt(args[4]);
			} catch (NumberFormatException e) {
				System.out.println("Given invalid bytes numBytes or reqSize. " + usage);
				return;
			}

			byte[] data = randomData(bufLen);
			
			appendFile(fname, data, dataSize, bufLen);
		} else if (cmd.equals("file")) {
			appendFile(fname, args[3]);
		} else {
			System.out.println(usage);
			return;
		}
	}

	private void appendFile(String fn, String srcFile) throws IOException, OutOfMemoryException,
			MaxFileSizeExceededException, InvalidFileOffsetException, KawkabException, InterruptedException {
		try (DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(new File(srcFile))))) {
			int bufLen = 4*1024;
			byte[] buf = new byte[bufLen];
			int dataSize = 0;
			while ((dataSize = dis.read(buf)) != -1) {
				appendFile(fn, buf, dataSize, buf.length);
			}
		}
	}

	private void appendFile(String fn, byte[] data, long dataSize, int bufLen) throws IOException, OutOfMemoryException,
			MaxFileSizeExceededException, InvalidFileOffsetException, KawkabException, InterruptedException {
		FileHandle file = null;
		file = openedFiles.get(fn);
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
			throws NumberFormatException, IbmapsFullException, IOException, KawkabException {
		String usage = "Usage: read filename offset intLength <dstFile>";
		
		String fn = null;
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
			throws IbmapsFullException, IOException, KawkabException {
		FileHandle file = null;
		file = openedFiles.get(fn);
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
		} catch (IllegalArgumentException | InterruptedException e) {
			e.printStackTrace();
		} finally {
			if (out != null)
				out.close();
		}
		System.out.println("");
	}

	private void parseOpen(String[] args) throws IOException, IbmapsFullException, KawkabException, InterruptedException, FileAlreadyOpenedException {
		if (args.length < 3) {
			System.out.println("Usage: open <filename> <r|a>");
			return;
		}

		openFile(args[1], args[2]);
	}

	private FileHandle openFile(String fn, String fm) throws IOException, IbmapsFullException, KawkabException, InterruptedException, FileAlreadyOpenedException {
		FileMode mode;
		if (fm.equals("r"))
			mode = FileMode.READ;
		else if (fm.equals("a"))
			mode = FileMode.APPEND;
		else
			throw new IOException("Invalid file mode");

		FileHandle fh = fs.open(fn, mode, new FileOptions());
		openedFiles.put(fn, fh);

		return fh;
	}
	
	private void shutdown() throws KawkabException, InterruptedException, IOException {
		fs.shutdown();
		System.out.println("Closed CLI");
		
		/*Set<Thread> threadSet = Thread.getAllStackTraces().keySet();
		for (Thread thr : threadSet) {
			System.out.println(thr.getName());
			StackTraceElement[] stes = thr.getStackTrace();
			for (StackTraceElement ste : stes) {
				System.out.println("\t"+ste);
			}
		}
		System.out.println("\n");*/
	}
	
	private Properties getProperties() throws IOException {
		String propsFile = "/config.properties";
		
		try (InputStream in = Thread.class.getResourceAsStream(propsFile)) {
			assert in != null;
			
			if (in == null) {
				System.out.println("in is null");
			}
			
			Properties props = new Properties();
			props.load(in);
			return props;
		}
	}
	
	private int getNodeID() throws KawkabException {
		String nodeIDProp = "nodeID";
		
		if (System.getProperty(nodeIDProp) == null) {
			throw new KawkabException("System property nodeID is not defined.");
		}
		
		return Integer.parseInt(System.getProperty(nodeIDProp));
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
