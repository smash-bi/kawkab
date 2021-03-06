package kawkab.fs.tests;

import kawkab.fs.api.FileOptions;
import kawkab.fs.commons.Configuration;
import kawkab.fs.commons.Stats;
import kawkab.fs.core.FileHandle;
import kawkab.fs.core.Filesystem;
import kawkab.fs.core.Filesystem.FileMode;
import kawkab.fs.core.exceptions.*;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;

public final class FSTest {
	private Configuration conf;

	public static void main(String args[]) throws InterruptedException, IOException, IllegalArgumentException, KawkabException {
		FSTest tester = new FSTest();
		tester.testBootstrap();
		tester.testBlocksCreation();
		tester.testSmallReadWrite();
		tester.testSmallRead();
		tester.testLargeReadWrite();
		tester.testMultipleReaders();
		tester.testMultipleFiles();
		tester.testWritePerformance();
		tester.testWritePerformanceConcurrentFiles();
		tester.testReadPerformance();
		tester.testReadPerfMultiReadersSameFile();

		tester.testConcurrentReadWrite();
		tester.testConcurrentReadWriteLastBlock();

		tester.testRawWrites();

		// tester.testVeryLargeReadWrite();
		// tester.testFileSeek();


		// Thread.sleep(5000);
		tester.shutdownTest();

// tester.testVeryLargeReadWrite();
// tester.testFileSeek();

		Thread.sleep(5000);
		tester.shutdownTest();
	}
	/*public void testConfiguration(){
		Constants.printConfig();
	}*/

	/**
	 * This must be called first to bootstrap the system.
	 *
	 * @throws AlreadyConfiguredException
	 */
	private void testBootstrap() throws IOException, KawkabException, InterruptedException {
		System.out.println("---------------------------------------------");
		System.out.println("            Bootstrap test");
		System.out.println("---------------------------------------------");

		int nodeID = Configuration.getNodeID();
		Properties props = Configuration.getProperties(Configuration.propsFileCluster);

		System.out.println("Node ID = " + nodeID);
		System.out.println("Loading properties from: " + props);

		Filesystem fs = Filesystem.bootstrap(nodeID, props);
		conf = fs.getConf();
	}

	/**
	 * Tests the creation of blocks for a new file.
	 *
	 * @throws AlreadyConfiguredException
	 */
	private void testBlocksCreation() throws IOException, KawkabException, InterruptedException {
		System.out.println("-------------------------------------------------");
		System.out.println("            Blocks creation test");
		System.out.println("-------------------------------------------------");

		Filesystem fs = Filesystem.instance();
		String filename = "testFile";
		FileHandle file = fs.open(filename, FileMode.APPEND, new FileOptions());

		byte[] data = new byte[10];
		file.append(data, 0, data.length);

		fs.close(file);
	}

	/**
	 * Test reading and writing a small file
	 */
	private void testSmallReadWrite() throws IOException, IllegalArgumentException, KawkabException, InterruptedException {
		System.out.println("--------------------------------------------");
		System.out.println("            Small file test read and write");
		System.out.println("--------------------------------------------");

		Filesystem fs = Filesystem.instance();

		String filename = "/home/smash/testSmall";
		FileOptions opts = new FileOptions();

		FileHandle file = fs.open(filename, FileMode.APPEND, opts);

		int dataSize = (int) (1.7 * conf.segmentSizeBytes);
		byte[] dataBuffer = new byte[dataSize];
		new Random().nextBytes(dataBuffer);

		System.out.println("Initial file size: " + file.size());

		long readOffset = file.size();
		int appended = file.append(dataBuffer, 0, dataBuffer.length);

		System.out.println(String.format("readOffset %d, file size %d, appended now %d", readOffset, file.size(), appended));

		byte[] readBuf = new byte[dataBuffer.length];
		int read = file.read(readBuf, readOffset, readBuf.length, true);

		fs.close(file);

		//System.out.println(Arrays.toString(readBuf));
		//System.out.println(Arrays.toString(dataBuffer));

		assert appended == read;
		assert Arrays.equals(dataBuffer, readBuf);
	}

	/**
	 * Tests reading an existing file.
	 */
	private void testSmallRead()
			throws IOException, IllegalArgumentException, KawkabException, InterruptedException {
		System.out.println("--------------------------------------------");
		System.out.println("            Small file test read");
		System.out.println("--------------------------------------------");

		Filesystem fs = Filesystem.instance();

		String filename = "/home/smash/testSmall";
		FileOptions opts = new FileOptions();

		FileHandle file = fs.open(filename, FileMode.READ, opts);

		int bufSize = conf.dataBlockSizeBytes;

		System.out.println("Initial file size: " + file.size());

		byte[] readBuf = new byte[bufSize];

		long fileSize = file.size();

		long read = 0;
		while (read != fileSize) {
			int len = fileSize-read > readBuf.length ? readBuf.length : (int)(fileSize-read);
			read += file.read(readBuf, 0, len, true);
		}

		System.out.println(String.format("file size %d, read bytes %d", file.size(), read));

		assert file.size() == read;

		fs.close(file);

		// System.out.println(Arrays.toString(readBuf));
		// System.out.println(Arrays.toString(dataBuffer));
	}

	/**
	 * Tests writing a large file.
	 */
	private void testLargeReadWrite() throws IOException, IllegalArgumentException, KawkabException, InterruptedException {
		System.out.println("--------------------------------------------");
		System.out.println("            Large files test");
		System.out.println("--------------------------------------------");

		Filesystem fs = Filesystem.instance();

		String filename = "/home/smash/testLarge";
		FileOptions opts = new FileOptions();
		FileHandle file = fs.open(filename, FileMode.APPEND, opts);

		System.out.println("Initial file size: " + file.size());

		int dataSize = 2 * conf.dataBlockSizeBytes + 1; //1*1024*1024*1024;
		byte[] dataBuffer = new byte[dataSize];
		new Random().nextBytes(dataBuffer);

		long readOffset = file.size();
		int appended = file.append(dataBuffer, 0, dataBuffer.length);

		System.out.println(String.format("readOffset %d, file size %d, appended now %d", readOffset, file.size(), appended));

		byte[] readBuf = new byte[dataBuffer.length];
		int read = file.read(readBuf, readOffset, readBuf.length, true);

		System.out.println("File " + filename + " size " + file.size());

		fs.close(file);

		assert appended == read;
		assert Arrays.equals(dataBuffer, readBuf);
	}

	/**
	 * Tests multiple readers concurrently reading the same file
	 */
	private void testMultipleReaders() throws IOException, KawkabException, InterruptedException {
		System.out.println("--------------------------------------------");
		System.out.println("            Multiple Readers Test");
		System.out.println("--------------------------------------------");

		Filesystem fs = Filesystem.instance();
		String filename = "/home/smash/multipleReaders";
		FileOptions opts = new FileOptions();
		FileHandle file = fs.open(filename, FileMode.APPEND, opts);
		final long offset = file.size();

		Random rand = new Random(0);
		int bufSize = conf.segmentSizeBytes;//8*1024*1024;
		long dataSize = 1L * 5 * conf.dataBlockSizeBytes + 1;
		long appended = 0;

		byte[] writeBuf = new byte[bufSize];
		rand.nextBytes(writeBuf);

		while (appended < dataSize) {
			int toWrite = (int) (appended + bufSize <= dataSize ? bufSize : dataSize - appended);
			appended += file.append(writeBuf, 0, toWrite);
			rand.nextBytes(writeBuf);
		}

		fs.close(file);

		System.out.println("Finished append. Staring readers.");

		int numReaders = 1;
		final long dataAppended = appended;

		Thread[] readers = new Thread[numReaders];
		for (int i = 0; i < readers.length; i++) {
			readers[i] = new Thread(() -> {
				byte[] writeBuf1 = new byte[bufSize];
				byte[] readBuf = new byte[bufSize];
				Random rand1 = new Random(0);
				//Random waitRand = new Random();

				FileHandle file1;
				try {
					file1 = fs.open(filename, FileMode.READ, opts);
				} catch (IOException | KawkabException | InterruptedException e) {
					e.printStackTrace();
					return;
				}
				long nextOffset = offset;

				long read = 0;
				while (read < dataSize) {
					int toRead = (int) (read + bufSize < dataSize ? bufSize : dataSize - read);
					int bytes;
					try {
						bytes = file1.read(readBuf, nextOffset, toRead, true);
						nextOffset += bytes;
					} catch (IOException | IllegalArgumentException | KawkabException e) {
						e.printStackTrace();
						break;
					}
					read += bytes;

					rand1.nextBytes(writeBuf1);
					if (bytes < writeBuf1.length) {
						for (int i1 = 0; i1 < bytes; i1++) {
							assert readBuf[i1] == writeBuf1[i1];
						}
					} else {
						assert Arrays.equals(readBuf, writeBuf1);
					}

					//LockSupport.parkNanos(waitRand.nextInt(90000));
				}

				try {
					fs.close(file1);
				} catch (KawkabException e) {
					e.printStackTrace();
				}

				assert dataAppended == read;
			});

			readers[i].start();
		}

		for (Thread reader : readers) {
			try {
				reader.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Tests concurrently reading and writing different files
	 */
	private void testMultipleFiles() throws IOException, KawkabException {
		System.out.println("--------------------------------------------");
		System.out.println("            Multiple Files Test");
		System.out.println("--------------------------------------------");

		Filesystem fs = Filesystem.instance();

		int numFiles = 100;
		Thread[] workers = new Thread[numFiles];

		for (int i = 0; i < numFiles; i++) {
			final int id = i;
			workers[i] = new Thread(() -> {
				try {
					String filename = "/home/smash/testMultipleFiles-" + id;
					FileOptions opts = new FileOptions();
					FileHandle file = fs.open(filename, FileMode.APPEND, opts);
					long readOffset = file.size();

					//System.out.println("Opening file: " + filename + ", current size="+initialSize);

					Random rand = new Random(0);
					int bufSize = conf.segmentSizeBytes;//8*1024*1024;
					long dataSize = 1L * conf.dataBlockSizeBytes + 1;
					long appended = 0;

					byte[] writeBuf = new byte[bufSize];
					rand.nextBytes(writeBuf);

					while (appended < dataSize) {
						int toWrite = (int) (appended + bufSize <= dataSize ? bufSize : dataSize - appended);
						int bytes = file.append(writeBuf, 0, toWrite);
						appended += bytes;
						rand.nextBytes(writeBuf);
					}

					byte[] readBuf = new byte[bufSize];
					rand = new Random(0);

					long read = 0;
					while (read < dataSize) {
						int toRead = (int) (read + bufSize < dataSize ? bufSize : dataSize - read);
						int bytes = file.read(readBuf, readOffset, toRead, true);
						read += bytes;
						readOffset += bytes;

						rand.nextBytes(writeBuf);
						if (bytes < bufSize) {
							for (int i1 = 0; i1 < bytes; i1++) {
								assert readBuf[i1] == writeBuf[i1];
							}
						} else {
							assert Arrays.equals(readBuf, writeBuf);
						}
					}

					fs.close(file);

					assert appended == read;
				} catch (Exception e) {
					e.printStackTrace();
					return;
				}
			});

			workers[i].start();
		}

		for (Thread worker : workers) {
			try {
				worker.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

	}

	/**
	 * Tests the performance of writing a single file
	 */
	private void testWritePerformance() throws IOException, KawkabException, InterruptedException {
		System.out.println("--------------------------------------------");
		System.out.println("            Write Performance Test");
		System.out.println("--------------------------------------------");
		Filesystem fs = Filesystem.instance();
		String filename = "/home/smash/writePerformanceTest";
		FileOptions opts = new FileOptions();
		FileHandle file = fs.open(filename, FileMode.APPEND, opts);

		Random rand = new Random(0);
		int bufSize = 1 * 1024; //Constants.segmentSizeBytes;
		long dataSize = 1000L * 1024 * 1024;
		long appended = 0;

		byte[] writeBuf = new byte[bufSize];
		rand.nextBytes(writeBuf);

		long initSize = file.size();

		long startTime = System.currentTimeMillis();
		while (appended < dataSize) {
			int toWrite = (int) (appended + bufSize <= dataSize ? bufSize : dataSize - appended);
			appended += file.append(writeBuf, 0, toWrite);
			//rand.nextBytes(writeBuf);
		}
		double durSec = (System.currentTimeMillis() - startTime) / 1000.0;
		double sizeMB = appended / 1024.0 / 1024.0;
		double speed = sizeMB / durSec;

		System.out.println(String.format("Req buffer size=%d, Data size=%.0fMB, Write speed=%.0fMB/s, File size=%dMB, initFileSize=%d, fileID=%d", bufSize, sizeMB, speed, file.size() / 1024 / 1024, initSize, file.inumber()));

		fs.close(file);
	}

	/**
	 * Tests the performance of append operations using multiple concurrent writers
	 */
	private void testWritePerformanceConcurrentFiles() throws IOException, KawkabException {
		System.out.println("----------------------------------------------------------------");
		System.out.println("            Write Perofrmance Test - Concurrent Files");
		System.out.println("----------------------------------------------------------------");

		Filesystem fs = Filesystem.instance();

		int numWriters = 1;
		Thread[] workers = new Thread[numWriters];
		final int bufSize = 1 * 100; //Constants.segmentSizeBytes;//8*1024*1024;
		final long dataSize = 25L * 1024 * 1024 * 1024;

		Stats writeStats = new Stats();
		for (int i = 0; i < numWriters; i++) {
			final int id = i;
			workers[i] = new Thread(() -> {
				try {
					String filename = "/home/smash/twpcf-" + id;
					FileOptions opts = new FileOptions();
					FileHandle file = fs.open(filename, FileMode.APPEND, opts);

					System.out.println("Opening file: " + filename + ", current size=" + file.size());

					Random rand = new Random(0);
					long appended = 0;

					final byte[] writeBuf = new byte[bufSize];
					rand.nextBytes(writeBuf);

					long startTime = System.currentTimeMillis();
					while (appended < dataSize) {
						int toWrite = (int) (appended + bufSize <= dataSize ? bufSize : dataSize - appended);
						appended += file.append(writeBuf, 0, toWrite);
					}

					double durSec = (System.currentTimeMillis() - startTime) / 1000.0;
					double sizeMB = appended / (1024.0 * 1024);
					double thr = sizeMB / durSec;
					writeStats.putValue(thr);

					fs.close(file);

					System.out.println(String.format("File %d: Data size = %.0fMB, Write thr = %.0fMB/s", id, sizeMB, thr));
				} catch (Exception e) {
					e.printStackTrace();
					return;
				}
			});

			workers[i].start();
		}

		for (Thread worker : workers) {
			try {
				worker.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		System.out.printf("WriteStats: sum=%.0f, %s, bufSize=%d\n", writeStats.sum(), writeStats, bufSize);
	}


	/**
	 * Tests the performance of reads on the primary node
	 */
	private void testReadPerformance() throws IOException, IllegalArgumentException, KawkabException, InterruptedException {
		System.out.println("--------------------------------------------");
		System.out.println("            Read Performance Test");
		System.out.println("--------------------------------------------");
		Filesystem fs = Filesystem.instance();
		String filename = "/home/smash/rp";
		FileOptions opts = new FileOptions();
		FileHandle file = fs.open(filename, FileMode.APPEND, opts);

		Random rand = new Random(0);
		int bufSize = conf.segmentSizeBytes;
		long dataSize = 2L * 1024 * 1024 * 1024;//1000*1000L*64L*bufSize + 3;
		byte[] buffer = new byte[bufSize];
		long appended = file.size();

		while (appended < dataSize) {
			int toWrite = (int) (appended + bufSize <= dataSize ? bufSize : dataSize - appended);
			rand.nextBytes(buffer);
			appended += file.append(buffer, 0, toWrite);
			//rand.nextBytes(writeBuf);
		}

		long startTime = System.currentTimeMillis();

		long read = 0;
		while (read < dataSize) {
			int toRead = (int) (read + bufSize < dataSize ? bufSize : dataSize - read);
			int bytes = file.read(buffer, read, toRead, true);
			read += bytes;
		}

		double durSec = (System.currentTimeMillis() - startTime) / 1000.0;
		double dataMB = read * 1.0 / 1024 / 1024;
		double speed = dataMB / durSec;

		fs.close(file);

		System.out.println(String.format("Data size = %.2fMB, Read speed = %.0fMB/s", dataMB, speed));
	}


	/**
	 * Read performance of the same file concurrently read by multiple readers
	 */
	private void testReadPerfMultiReadersSameFile() throws IOException, KawkabException, InterruptedException {
		System.out.println("-----------------------------------------------------------------");
		System.out.println("       Read Performance Multiple Readers Same File Test");
		System.out.println("-----------------------------------------------------------------");

		Filesystem fs = Filesystem.instance();
		String filename = "/home/smash/rpmsf";
		FileOptions opts = new FileOptions();
		FileHandle file = fs.open(filename, FileMode.APPEND, opts);

		Random rand = new Random(0);
		int bufSize = 10 * 1024;//Constants.segmentSizeBytes;//8*1024*1024;
		long dataSize = 1L * 1024 * 1024 * 1024;//1000*1000L*64L*bufSize + 3;
		long appended = file.size();

		byte[] writeBuf = new byte[bufSize];
		rand.nextBytes(writeBuf);

		while (appended < dataSize) {
			int toWrite = (int) (appended + bufSize <= dataSize ? bufSize : dataSize - appended);
			appended += file.append(writeBuf, 0, toWrite);
			rand.nextBytes(writeBuf);
		}

		fs.close(file);

		System.out.println("Finished append. Staring readers.");

		long startTime = System.currentTimeMillis();
		final Stats readStats = new Stats();
		int numReaders = 20;
		Thread[] readers = new Thread[numReaders];
		for (int i = 0; i < readers.length; i++) {
			readers[i] = new Thread(() -> {
				byte[] readBuf = new byte[bufSize];

				FileHandle file1;
				try {
					file1 = fs.open(filename, FileMode.READ, opts);
				} catch (IOException | KawkabException | InterruptedException e) {
					e.printStackTrace();
					return;
				}

				long startTime1 = System.currentTimeMillis();

				long read = 0;
				while (read < dataSize) {
					int toRead = (int) (read + bufSize < dataSize ? bufSize : dataSize - read);
					int bytes;
					try {
						bytes = file1.read(readBuf, read, toRead, true);
					} catch (IOException | IllegalArgumentException | KawkabException e) {
						e.printStackTrace();
						break;
					}
					read += bytes;
				}

				double durSec = (System.currentTimeMillis() - startTime1) / 1000.0;
				double dataMB = (1.0 * read) / (1024 * 1024);
				readStats.putValue(dataMB / durSec);

				try {
					fs.close(file1);
				} catch (KawkabException e) {
					e.printStackTrace();
				}

				System.out.println(String.format("Data size: %.2f, Read Speed: %.2fMB/s", dataMB, dataMB / durSec));
			});

			readers[i].start();
		}

		for (Thread reader : readers) {
			reader.join();
		}
		double durSec = (System.currentTimeMillis() - startTime) / 1000.0;

		System.out.println("Aggregate read throughput (MB/s): " + (dataSize / 1024.0 / 1024 * numReaders / durSec));
		System.out.printf("Read stats (MB/s): sum=%.0f %s\n", readStats.sum(), readStats);
	}

	/**
	 * Tests the performance of concurrent read and append operations
	 */
	private void testConcurrentReadWrite() throws IOException, KawkabException, InterruptedException {
		System.out.println("-----------------------------------------------------------------");
		System.out.println("       Concurrent Read Write Test");
		System.out.println("-----------------------------------------------------------------");

		final int numWorkers = 2;
		final int numFiles = 100;
		final int numTasks = 1000;
		final int appendSize = 10000;
		final int bufferSize = 1 * 1024;
		Thread[] workers = new Thread[numWorkers];
		final Random rand = new Random(conf.thisNodeID);
		final String prefix = "CRWTestL-";

		Filesystem.instance();

		for (int i = 0; i < numWorkers; i++) {
			final int id = i;
			workers[i] = new Thread("Test-worker-" + i) {
				int workerID = id;

				public void run() {
					try {
						Filesystem fs = Filesystem.instance();
						byte[] buffer = new byte[bufferSize];

						for (int nTask = 0; nTask < numTasks; nTask++) {
							FileHandle file;
							String fname = prefix + rand.nextInt(numFiles);
							FileMode mode = rand.nextBoolean() ? FileMode.APPEND : FileMode.READ;

							try {
								file = fs.open(fname, mode, new FileOptions());
							} catch (InvalidFileModeException | FileAlreadyOpenedException | FileNotExistException e) {
								//System.out.println("\t<"+workerID+"> Skipping file: " + fname);
								nTask--;
								continue;
							}

							if (mode == FileMode.APPEND) {
								long sizeMB = (long) ((file.size() + appendSize) / 1024.0 / 1024.0);

								System.out.println("\t<" + workerID + "> Task: " + nTask + ", Writing file: " + fname + " up to " + sizeMB + " MB");
								int appended = 0;
								while (appended < appendSize) {
									int toWrite = appended + bufferSize <= appendSize ? bufferSize : appendSize - appended;
									rand.nextBytes(buffer);
									appended += file.append(buffer, 0, toWrite);
									//rand.nextBytes(writeBuf);
								}
							} else {
								long dataSize = file.size();
								long read = 0;

								System.out.println("\t<" + workerID + "> Task: " + nTask + ", Reading file: " + fname + " up to " + (dataSize / 1024 / 1024) + " MB");

								while (read < dataSize) {
									int toRead = (int) (read + bufferSize < dataSize ? bufferSize : dataSize - read);
									int bytes;
									try {
										bytes = file.read(buffer, read, toRead, true);
									} catch (IOException | IllegalArgumentException | KawkabException e) {
										e.printStackTrace();
										break;
									}
									read += bytes;
								}
							}

							fs.close(file);
						}
					} catch (KawkabException | IOException | InterruptedException e) {
						e.printStackTrace();
					}
				}
			};

			workers[i].start();
		}

		for (int i = 0; i < numWorkers; i++) {
			workers[i].join();
		}
	}


	private void testConcurrentReadWriteLastBlock() throws InterruptedException {
		System.out.println("-----------------------------------------------------------------");
		System.out.println("       Concurrent Read Write of Last Block Test");
		System.out.println("-----------------------------------------------------------------");

		final int numWorkers = 100;
		final int numFiles = 1;
		final int numTasks = 1000;
		final int appendSize = conf.segmentSizeBytes / 3; // An odd append size to test reading existing block as well as newly created block
		final int bufferSize = 1024;
		Thread[] workers = new Thread[numWorkers];
		final Random rand = new Random(conf.thisNodeID);

		for (int i = 0; i < numWorkers; i++) {
			final int id = i;
			workers[i] = new Thread("Test-worker-" + i) {
				int workerID = id;

				public void run() {
					try {
						Filesystem fs = Filesystem.instance();
						byte[] buffer = new byte[bufferSize];

						String fname = "CRWLBTest-" + rand.nextInt(numFiles);
						FileHandle file;
						FileMode mode = workerID == 0 ? FileMode.APPEND : FileMode.READ;

						try {
							file = fs.open(fname, mode, new FileOptions());
						} catch (InvalidFileModeException | FileAlreadyOpenedException | FileNotExistException e) {
							e.printStackTrace();
							return;
						}

						if (workerID != 0) {
							Thread.sleep(1000);
						}


						for (int nTask = 0; nTask < numTasks; nTask++) {
							if (mode == FileMode.APPEND) {
								long sizeMB = (file.size() + appendSize) / 1024 / 1024;

								System.out.println("\t<" + workerID + "> Task: " + nTask + ", Writing file: " + fname + " up to " + sizeMB + " MB");
								int appended = 0;
								while (appended < appendSize) {
									int toWrite = appended + bufferSize <= appendSize ? bufferSize : appendSize - appended;
									rand.nextBytes(buffer);
									appended += file.append(buffer, 0, toWrite);
									//rand.nextBytes(writeBuf);
								}
							} else {
								long dataSize = 100;
								long sizeMB = (file.size() - dataSize) / 1024 / 1024;
								long read = 0;
								long readOffset = file.size() - dataSize;
								System.out.println("\t<" + workerID + "> Task: " + nTask + ", Reading file: " + fname + " up to " + sizeMB + " MB");

								while (read < dataSize) {
									int toRead = (int) (read + bufferSize < dataSize ? bufferSize : dataSize - read);
									int bytes = 0;
									try {
										bytes = file.read(buffer, readOffset, toRead, true);
									} catch (IOException | IllegalArgumentException | KawkabException e) {
										e.printStackTrace();
										break;
									}
									read += bytes;
									readOffset += bytes;
								}
							}
						}

						fs.close(file);

					} catch (KawkabException | IOException | InterruptedException e) {
						e.printStackTrace();
					}
				}
			};

			workers[i].start();

			if (i == 0) { //Allow the writer to write some data
				Thread.sleep(1000);
			}
		}

		for (int i = 0; i < numWorkers; i++) {
			workers[i].join();
		}
	}

	private void testRawWrites() {
		System.out.println("----------------------------------------------------------------");
		System.out.println("            Raw writes Test using ByteBuffer");
		System.out.println("----------------------------------------------------------------");

		int numWriters = 1;
		Thread[] workers = new Thread[numWriters];
		final int bufSize = 1 * 100; //Constants.segmentSizeBytes;//8*1024*1024;
		final long dataSize = 1L * 1024 * 1024 * 1024;

		Stats writeStats = new Stats();
		for (int i = 0; i < numWriters; i++) {
			final int id = i;
			workers[i] = new Thread(() -> {
				try {
					byte[] writeBuf = new byte[bufSize];
					new Random().nextBytes(writeBuf);

					long appended = 0;
					long startTime = System.currentTimeMillis();

					ByteBuffer dataBuf = ByteBuffer.allocateDirect(conf.segmentSizeBytes);
					while (appended < dataSize) {
						int toWrite = (int) (appended + bufSize <= dataSize ? bufSize : dataSize - appended);

						if (toWrite > dataBuf.remaining()) {
							dataBuf = ByteBuffer.allocateDirect(conf.segmentSizeBytes);
							assert toWrite <= dataBuf.remaining();
						}

						dataBuf.position(0);
						dataBuf.put(writeBuf, 0, toWrite);
						appended += toWrite;
					}

					double durSec = (System.currentTimeMillis() - startTime) / 1000.0;
					double sizeMB = appended / (1024.0 * 1024);
					double thr = sizeMB / durSec;
					writeStats.putValue(thr);

					System.out.println(String.format("File %d: Data size = %.0fMB, Write thr = %.0fMB/s", id, sizeMB, thr));
				} catch (Exception e) {
					e.printStackTrace();
					return;
				}
			});

			workers[i].start();
		}

		for (Thread worker : workers) {
			try {
				worker.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		System.out.printf("WriteStats: sum=%.0f, %s, bufSize=%d\n", writeStats.sum(), writeStats, bufSize);
	}

	/**
	 * This must be called at the end to flush any data left in the cache and to finish sending data to the global store.
	 *
	 * @throws KawkabException
	 * @throws InterruptedException
	 * @throws IOException
	 */
	private void shutdownTest() throws KawkabException, InterruptedException, IOException {
		System.out.println("--------------------------------------------");
		System.out.println("            Filesystem shutdown Test");
		System.out.println("--------------------------------------------");

		Filesystem.instance().shutdown();
	}

	@Test
	public void mainTest() throws InterruptedException, IOException, IllegalArgumentException, KawkabException {
		FSTest.main(null);
	}
}
