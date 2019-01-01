package kawkab.fs.cli;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import kawkab.fs.api.FileHandle;
import kawkab.fs.api.FileOptions;
import kawkab.fs.commons.Constants;
import kawkab.fs.core.Filesystem;
import kawkab.fs.core.Filesystem.FileMode;
import kawkab.fs.core.exceptions.IbmapsFullException;
import kawkab.fs.core.exceptions.InvalidFileOffsetException;
import kawkab.fs.core.exceptions.KawkabException;
import kawkab.fs.core.exceptions.MaxFileSizeExceededException;
import kawkab.fs.core.exceptions.OutOfMemoryException;

public final class CLI {
	private Filesystem fs;
	private static final int BUFLEN = Constants.dataBlockSizeBytes;
	private Map<String, FileHandle> openedFiles;

	public CLI() {
		openedFiles = new HashMap<String, FileHandle>();
	}
	
	public static void main(String[] args) throws IOException, KawkabException, InterruptedException {
		CLI c = new CLI();

		c.initFS();
		c.cmd();
		c.shutdown();
	}

	private void shutdown() throws KawkabException, InterruptedException, IOException {
		fs.shutdown();
	}

	private void initFS() throws IOException, KawkabException, InterruptedException {
		//Constants.printConfig();
		fs = Filesystem.instance();
		fs.bootstrap();
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
		String usage = "Usage: apnd <filename> <str <string>> | file <filepath> | bytes <numBytes>";
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
			int len = -1;
			try {
				len = Integer.parseInt(args[3]);
			} catch (NumberFormatException e) {
				System.out.println("Given invalid bytes numBytes. " + usage);
				return;
			}
			
			String chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
			int charLen = chars.length();
			Random rand = new Random();
			byte[] data = new byte[len];
			for (int i=0; i<data.length; i++) {
				data[i] = (byte)chars.charAt(rand.nextInt(charLen));
			}
			appendFile(fname, data, 0, len);
		} else if (cmd.equals("file")) {
			appendFile(fname, args[3]);
		} else {
			System.out.println(usage);
			return;
		}
	}

	private void appendFile(String fn, String srcFile) throws IOException, OutOfMemoryException,
			MaxFileSizeExceededException, InvalidFileOffsetException, KawkabException, InterruptedException {
		DataInputStream di = null;
		try {
			di = new DataInputStream(new BufferedInputStream(new FileInputStream(new File(srcFile))));
			byte[] buf = new byte[Constants.dataBlockSizeBytes];
			int n = 0;
			while ((n = di.read(buf)) != -1) {
				appendFile(fn, buf, 0, n);
			}
		} finally {
			if (di != null) {
				di.close();
			}
		}
	}

	private void appendFile(String fn, byte[] data, int offset, int len) throws IOException, OutOfMemoryException,
			MaxFileSizeExceededException, InvalidFileOffsetException, KawkabException, InterruptedException {
		FileHandle file = null;
		file = openedFiles.get(fn);
		if (file == null) {
			throw new IOException("File not opened: " + fn);
		}

		file.append(data, offset, len);
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

		byte[] buf = new byte[BUFLEN];
		long read = 0;
		BufferedWriter out = null;
		try {
			if (dst != null) {
				out = new BufferedWriter(new FileWriter(new File(dst)));
			}

			long length = file.size();
			if (readLen > 0) {
				length = readLen;
			}

			file.seekBytes(byteStart);
			System.out.println("[CLI] read len bytes: " + length);
			while (read < length) {
				int toRead = (int) (length - read >= buf.length ? buf.length : length - read);
				int len = file.read(buf, toRead);
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

	private void parseOpen(String[] args) throws IOException, IbmapsFullException, KawkabException, InterruptedException {
		if (args.length < 3) {
			System.out.println("Usage: open <filename> <r|a>");
			return;
		}

		openFile(args[1], args[2]);
	}

	private FileHandle openFile(String fn, String fm) throws IOException, IbmapsFullException, KawkabException, InterruptedException {
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
}