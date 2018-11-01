package kawkab.fs.core;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import kawkab.fs.api.FileHandle;
import kawkab.fs.api.FileOptions;
import kawkab.fs.commons.Constants;
import kawkab.fs.commons.Stats;
import kawkab.fs.core.Filesystem.FileMode;
import kawkab.fs.core.exceptions.FileNotExistException;
import kawkab.fs.core.exceptions.IbmapsFullException;
import kawkab.fs.core.exceptions.InvalidFileModeException;
import kawkab.fs.core.exceptions.InvalidFileOffsetException;
import kawkab.fs.core.exceptions.KawkabException;
import kawkab.fs.core.exceptions.MaxFileSizeExceededException;
import kawkab.fs.core.exceptions.OutOfMemoryException;

public final class FSTest {
	public void testMain() throws OutOfMemoryException, MaxFileSizeExceededException, 
				InterruptedException, IbmapsFullException, InvalidFileOffsetException, 
				InvalidFileModeException, IOException, IllegalArgumentException, KawkabException {
		FSTest.main(null);
	}
	
	public static void main(String args[]) throws OutOfMemoryException, 
				MaxFileSizeExceededException, InterruptedException, IbmapsFullException, 
				InvalidFileOffsetException, InvalidFileModeException, IOException, IllegalArgumentException, KawkabException {
		FSTest tester = new FSTest();
		//Constants.printConfig();
		tester.testBootstrap();
		//tester.testBlocksCreation();
		//tester.testSmallReadWrite();
		//tester.testSmallRead();
		tester.testLargeReadWrite();
		//tester.testMultipleReaders();
		//tester.testMultipleFiles();
		//tester.testWritePerformance();
		tester.testWritePerformanceConcurrentFiles();
		//tester.testReadPerformance();
		//tester.testReadPerfMultiReadersSameFile();
		
		//tester.testConcurrentReadWrite();
		
		//tester.testVeryLargeReadWrite();
		//tester.testFileSeek();
		
		Thread.sleep(5000);
		tester.testShutdown();
	}
	
	/*public void testConfiguration(){
		Constants.printConfig();
	}*/
	
	private void testBootstrap() throws IOException, KawkabException, InterruptedException{
		System.out.println("---------------------------------------------");
		System.out.println("            Bootstrap test");
		System.out.println("---------------------------------------------");
		
		Filesystem fs = Filesystem.instance();
		fs.bootstrap();
	}
	
	private void testBlocksCreation() throws IbmapsFullException, OutOfMemoryException, 
				MaxFileSizeExceededException, InvalidFileOffsetException, InvalidFileModeException, IOException, KawkabException, InterruptedException{
		System.out.println("-------------------------------------------------");
		System.out.println("            Blocks creation test");
		System.out.println("-------------------------------------------------");
		
		Filesystem fs = Filesystem.instance().bootstrap();
		String filename = "testFile";
		FileHandle file = fs.open(filename, FileMode.APPEND, new FileOptions());
		
		byte[] data = new byte[10];
		file.append(data, 0, data.length);
	}
	
	private void testSmallReadWrite() throws OutOfMemoryException, MaxFileSizeExceededException, IbmapsFullException, 
				InvalidFileOffsetException, InvalidFileModeException, IOException, IllegalArgumentException, KawkabException, InterruptedException{
		System.out.println("--------------------------------------------");
		System.out.println("            Small file test read and write");
		System.out.println("--------------------------------------------");
		
		Filesystem fs = Filesystem.instance().bootstrap();
		
		String filename = new String("/home/smash/testSmall");
		FileOptions opts = new FileOptions();
		
		FileHandle file = fs.open(filename, FileMode.APPEND, opts);
		
		int dataSize = (int)(1.7*Constants.segmentSizeBytes);
		byte[] dataBuffer = new byte[dataSize];
		new Random().nextBytes(dataBuffer);
		
		System.out.println("Initial file size: " + file.size());
		
		file.seekBytes(file.size());
		int appended = file.append(dataBuffer, 0, dataBuffer.length);
		
		System.out.println(String.format("readOffset %d, file size %d, appended now %d", file.readOffset(), file.size(), appended));
		
		byte[] readBuf = new byte[dataBuffer.length];
		int read = file.read(readBuf, readBuf.length);
		
		
		//System.out.println(Arrays.toString(readBuf));
		//System.out.println(Arrays.toString(dataBuffer));

		assert appended == read;
		assert Arrays.equals(dataBuffer, readBuf);
	}
	
	private void testSmallRead()
			throws OutOfMemoryException, MaxFileSizeExceededException, IbmapsFullException, InvalidFileOffsetException,
			InvalidFileModeException, IOException, IllegalArgumentException, KawkabException, InterruptedException {
		System.out.println("--------------------------------------------");
		System.out.println("            Small file test read");
		System.out.println("--------------------------------------------");

		Filesystem fs = Filesystem.instance().bootstrap();

		String filename = new String("/home/smash/testSmall");
		FileOptions opts = new FileOptions();

		FileHandle file = fs.open(filename, FileMode.READ, opts);

		int bufSize = Constants.dataBlockSizeBytes;

		System.out.println("Initial file size: " + file.size());

		file.seekBytes(0);

		byte[] readBuf = new byte[bufSize];
		
		long fileSize = file.size();
		long read = 0;
		while(read != fileSize) {
			read += file.read(readBuf, readBuf.length);
		}

		// System.out.println(Arrays.toString(readBuf));
		// System.out.println(Arrays.toString(dataBuffer));

		System.out.println(String.format("Current readOffset %d, file size %d, read bytes %d", file.readOffset(), file.size(), read));
	}
	
	private void testLargeReadWrite() throws OutOfMemoryException, MaxFileSizeExceededException, IbmapsFullException, 
				InvalidFileOffsetException, InvalidFileModeException, IOException, IllegalArgumentException, KawkabException, InterruptedException{
		System.out.println("--------------------------------------------");
		System.out.println("            Large files test");
		System.out.println("--------------------------------------------");
		
		Filesystem fs = Filesystem.instance().bootstrap();
		
		String filename = new String("/home/smash/testLarge");
		FileOptions opts = new FileOptions();
		FileHandle file = fs.open(filename, FileMode.APPEND, opts);
		
		System.out.println("Initial file size: " + file.size());
		
		int dataSize = 2*Constants.dataBlockSizeBytes + 1; //1*1024*1024*1024;
		byte[] dataBuffer = new byte[dataSize];
		new Random().nextBytes(dataBuffer);
		
		file.seekBytes(file.size());
		int appended = file.append(dataBuffer, 0, dataBuffer.length);
		
		System.out.println(String.format("readOffset %d, file size %d, appended now %d", file.readOffset(), file.size(), appended));
		
		byte[] readBuf = new byte[dataBuffer.length];
		int read = file.read(readBuf, readBuf.length);
		
		System.out.println("File " + filename + " size " + file.size());
		
		assert appended == read;
		assert Arrays.equals(dataBuffer, readBuf);
	}
	
	private void testMultipleReaders() throws IbmapsFullException, OutOfMemoryException, 
	MaxFileSizeExceededException, InvalidFileOffsetException, InvalidFileModeException, IOException, KawkabException, InterruptedException {
		System.out.println("--------------------------------------------");
		System.out.println("            Multiple Readers Test");
		System.out.println("--------------------------------------------");
		
		Filesystem fs = Filesystem.instance().bootstrap();
		String filename = new String("/home/smash/multipleReaders");
		FileOptions opts = new FileOptions();
		FileHandle file = fs.open(filename, FileMode.APPEND, opts);
		final long offset = file.size();
		
		Random rand = new Random(0);
		int bufSize = Constants.segmentSizeBytes;//8*1024*1024;
		long dataSize = 1L*20*Constants.dataBlockSizeBytes + 1;
		long appended = 0;
		
		byte[] writeBuf = new byte[bufSize];
		rand.nextBytes(writeBuf);
		
		while(appended < dataSize) {
			int toWrite = (int)(appended+bufSize <= dataSize ? bufSize : dataSize - appended);
			appended += file.append(writeBuf, 0, toWrite);
			rand.nextBytes(writeBuf);
		}
		
		System.out.println("Finished append. Staring readers.");
		
		int numReaders = 20;
		final long dataAppended = appended;
		
		Thread[] readers = new Thread[numReaders];
		for (int i=0; i<readers.length; i++){
			readers[i] = new Thread() {
				public void run() {
					byte[] writeBuf = new byte[bufSize];
					byte[] readBuf = new byte[bufSize];
					Random rand = new Random(0);
					Random waitRand = new Random();
					
					FileHandle file = null;
					try {
						file = fs.open(filename, FileMode.READ, opts);
					} catch (IbmapsFullException | IOException | KawkabException | InterruptedException e) {
						e.printStackTrace();
						return;
					}
					file.seekBytes(offset);
					
					long read = 0;
					while(read < dataSize) {
						int toRead = (int)(read+bufSize < dataSize ? bufSize : dataSize - read);
						int bytes = 0;
						try {
							bytes = file.read(readBuf, toRead);
						} catch (IOException | IllegalArgumentException | KawkabException | InterruptedException e) {
							e.printStackTrace();
							break;
						}
						read += bytes;
						
						rand.nextBytes(writeBuf);
						if (bytes< writeBuf.length) {
							for(int i=0; i<bytes; i++){
								assert readBuf[i] == writeBuf[i];
							}
						} else {
							assert Arrays.equals(readBuf, writeBuf);
						}
						
						//LockSupport.parkNanos(waitRand.nextInt(90000));
					}
					
					assert dataAppended == read;
				}
			};
			
			readers[i].start();
		}
		
		for (int i=0; i<readers.length; i++){
			try {
				readers[i].join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	private void testMultipleFiles() throws IbmapsFullException, OutOfMemoryException, 
	MaxFileSizeExceededException, InvalidFileOffsetException, InvalidFileModeException, IOException, KawkabException, InterruptedException {
		System.out.println("--------------------------------------------");
		System.out.println("            Multiple Files Test");
		System.out.println("--------------------------------------------");
		
		Filesystem fs = Filesystem.instance().bootstrap();
		
		int numFiles = 100;
		Thread[] workers = new Thread[numFiles];
		
		for (int i=0; i<numFiles; i++) {
			final int id = i;
			workers[i] = new Thread(){
				public void run(){
					try {
						String filename = new String("/home/smash/testMultipleFiles-"+id);
						FileOptions opts = new FileOptions();
						FileHandle file = fs.open(filename, FileMode.APPEND, opts);
						long initialSize = file.size();
						
						System.out.println("Opening file: " + filename + ", current size="+initialSize);
						
						Random rand = new Random(0);
						int bufSize = Constants.segmentSizeBytes;//8*1024*1024;
						long dataSize = 4L*Constants.dataBlockSizeBytes + 17;
						long appended = 0;
						
						byte[] writeBuf = new byte[bufSize];
						rand.nextBytes(writeBuf);
						
						while(appended < dataSize) {
							int toWrite = (int)(appended+bufSize <= dataSize ? bufSize : dataSize - appended);
							int bytes = file.append(writeBuf, 0, toWrite);
							appended += bytes;
							rand.nextBytes(writeBuf);
						}
						
						byte[] readBuf = new byte[bufSize];
						rand = new Random(0);
						
						file.seekBytes(initialSize);
						long read = 0;
						while(read < dataSize) {
							int toRead = (int)(read+bufSize < dataSize ? bufSize : dataSize - read);
							int bytes = file.read(readBuf, toRead);
							read += bytes;
							
							rand.nextBytes(writeBuf);
							if (bytes < bufSize){
								for(int i=0; i<bytes; i++){
									assert readBuf[i] == writeBuf[i];
								}
							} else {
								assert Arrays.equals(readBuf, writeBuf);
							}
						}
						
						assert appended == read;
					} catch (Exception e){
						e.printStackTrace();
						return;
					}
				}
			};
			
			workers[i].start();
		}
		
		for (int i=0; i<workers.length; i++) {
			try {
				workers[i].join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
	}
	
	private void testWritePerformance() throws IOException, IbmapsFullException, OutOfMemoryException, 
			MaxFileSizeExceededException, InvalidFileOffsetException, InvalidFileModeException, KawkabException, InterruptedException{
		System.out.println("--------------------------------------------");
		System.out.println("            Write Performance Test");
		System.out.println("--------------------------------------------");
		Filesystem fs = Filesystem.instance().bootstrap();
		String filename = new String("/home/smash/writePerformanceTest");
		FileOptions opts = new FileOptions();
		FileHandle file = fs.open(filename, FileMode.APPEND, opts);
		
		Random rand = new Random(0);
		int bufSize = 10000*1024; //Constants.segmentSizeBytes;
		long dataSize = 1L*1024*1024*1024;//1000*1000L*64L*bufSize + 3;
		long appended = 0;
		
		byte[] writeBuf = new byte[bufSize];
		rand.nextBytes(writeBuf);
		
		long initSize = file.size();
		
		long startTime = System.currentTimeMillis();
		while(appended < dataSize) {
			int toWrite = (int)(appended+bufSize <= dataSize ? bufSize : dataSize - appended);
			appended += file.append(writeBuf, 0, toWrite);
			//rand.nextBytes(writeBuf);
		}
		double durSec = (System.currentTimeMillis() - startTime)/1000.0;
		double sizeMB = appended/1024.0/1024.0;
		double speed = sizeMB/durSec;
		
		System.out.println(String.format("Req buffer size=%d, Data size=%.0fMB, Write speed=%.0fMB/s, File size=%dMB, initFileSize=%d, fileID=%d", bufSize, sizeMB, speed, file.size()/1024/1024, initSize, file.inumber()));
	}
	
	private void testWritePerformanceConcurrentFiles() throws IbmapsFullException, OutOfMemoryException, 
	MaxFileSizeExceededException, InvalidFileOffsetException, InvalidFileModeException, IOException, KawkabException, InterruptedException {
		System.out.println("----------------------------------------------------------------");
		System.out.println("            Write Perofrmance Test - Concurrent Files");
		System.out.println("----------------------------------------------------------------");
		
		Filesystem fs = Filesystem.instance().bootstrap();
		
		int numWriters = 16;
		Thread[] workers = new Thread[numWriters];
		final int bufSize = 20*1024; //Constants.segmentSizeBytes;//8*1024*1024;
		final long dataSize = 1L*200*1024*1024;
		Stats writeStats = new Stats();
		
		long startTime = System.currentTimeMillis();
		for (int i=0; i<numWriters; i++) {
			final int id = i;
			workers[i] = new Thread(){
				public void run(){
					try {
						String filename = new String("/home/smash/testMultipleFiles-"+id);
						FileOptions opts = new FileOptions();
						FileHandle file = fs.open(filename, FileMode.APPEND, opts);
						
						System.out.println("Opening file: " + filename + ", current size="+file.size());
						
						Random rand = new Random(0);
						long appended = 0;
						
						byte[] writeBuf = new byte[bufSize];
						rand.nextBytes(writeBuf);
						
						long startTime = System.currentTimeMillis();
						
						Stats stats = new Stats();
						
						while(appended < dataSize) {
							int toWrite = (int)(appended+bufSize <= dataSize ? bufSize : dataSize - appended);
							long s = System.currentTimeMillis();
							appended += file.append(writeBuf, 0, toWrite);
							stats.putValue((System.currentTimeMillis()-s));
							//rand.nextBytes(writeBuf);
						}
						
						double durSec = (System.currentTimeMillis() - startTime)/1000.0;
						double sizeMB = appended/(1024.0*1024);
						double thr = sizeMB/durSec;
						writeStats.putValue(thr);
						
						System.out.println(String.format("File %d: Data size = %.0fMB, Write thr = %.0fMB/s, stats (ms): %s", id, sizeMB, thr, stats));
					} catch (Exception e){
						e.printStackTrace();
						return;
					}
				}
			};
			
			workers[i].start();
		}
		
		for (int i=0; i<workers.length; i++) {
			try {
				workers[i].join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		long elapsed = System.currentTimeMillis() - startTime;
		
		int thr = (int)((dataSize/(1024.0*1024)*numWriters)/(elapsed/1000.0));
		
		System.out.println("Aggregate write thr (MB/s) = " + thr);
		System.out.printf("WriteStats: sum=%.0f, %s, bufSize=%d\n",writeStats.sum(), writeStats, bufSize);
	}
	
	private void testReadPerformance() throws IOException, IbmapsFullException, OutOfMemoryException, 
		MaxFileSizeExceededException, InvalidFileOffsetException, InvalidFileModeException, IllegalArgumentException, KawkabException, InterruptedException{
		System.out.println("--------------------------------------------");
		System.out.println("            Read Performance Test");
		System.out.println("--------------------------------------------");
		Filesystem fs = Filesystem.instance().bootstrap();
		String filename = new String("/home/smash/readPerformanceTest");
		FileOptions opts = new FileOptions();
		FileHandle file = fs.open(filename, FileMode.APPEND, opts);
		
		Random rand = new Random(0);
		int bufSize = Constants.segmentSizeBytes;
		long dataSize = 2L*1024*1024*1024;//1000*1000L*64L*bufSize + 3;
		byte[] buffer = new byte[bufSize];
		long appended = file.size();
		
		while(appended < dataSize) {
			int toWrite = (int)(appended+bufSize <= dataSize ? bufSize : dataSize - appended);
			rand.nextBytes(buffer);
			appended += file.append(buffer, 0, toWrite);
			//rand.nextBytes(writeBuf);
		}
		
		file.seekBytes(0);
		
		long startTime = System.currentTimeMillis();
		
		long read = 0;
		while(read < dataSize) {
			int toRead = (int)(read+bufSize < dataSize ? bufSize : dataSize - read);
			int bytes = file.read(buffer, toRead);
			read += bytes;
		}
		
		double durSec = (System.currentTimeMillis() - startTime)/1000.0;
		double dataMB = read*1.0/1024/1024;
		double speed = dataMB/durSec;
		
		System.out.println(String.format("Data size = %.2fMB, Read speed = %.0fMB/s", dataMB, speed));
	}
	
	private void testReadPerfMultiReadersSameFile() throws IbmapsFullException, OutOfMemoryException, 
	MaxFileSizeExceededException, InvalidFileOffsetException, InvalidFileModeException, IOException, KawkabException, InterruptedException {
		System.out.println("-----------------------------------------------------------------");
		System.out.println("       Read Performance Multiple Readers Same File Test");
		System.out.println("-----------------------------------------------------------------");
		
		Filesystem fs = Filesystem.instance().bootstrap();
		String filename = new String("/home/smash/multipleReaders");
		FileOptions opts = new FileOptions();
		FileHandle file = fs.open(filename, FileMode.APPEND, opts);
		
		Random rand = new Random(0);
		int bufSize = 10*1024;//Constants.segmentSizeBytes;//8*1024*1024;
		long dataSize = 1L*1024*1024*1024;//1000*1000L*64L*bufSize + 3;
		long appended = file.size();
		
		byte[] writeBuf = new byte[bufSize];
		rand.nextBytes(writeBuf);
		
		while(appended < dataSize) {
			int toWrite = (int)(appended+bufSize <= dataSize ? bufSize : dataSize - appended);
			appended += file.append(writeBuf, 0, toWrite);
			rand.nextBytes(writeBuf);
		}
		
		System.out.println("Finished append. Staring readers.");
		
		long startTime = System.currentTimeMillis();
		final Stats readStats = new Stats();
		int numReaders = 8;
		Thread[] readers = new Thread[numReaders];
		for (int i=0; i<readers.length; i++){
			readers[i] = new Thread() {
				public void run() {
					byte[] readBuf = new byte[bufSize];
					
					FileHandle file = null;
					try {
						file = fs.open(filename, FileMode.READ, opts);
					} catch (IbmapsFullException | IOException | KawkabException | InterruptedException e) {
						e.printStackTrace();
						return;
					}
					file.seekBytes(0);
					
					long startTime = System.currentTimeMillis();
					
					long read = 0;
					while(read < dataSize) {
						int toRead = (int)(read+bufSize < dataSize ? bufSize : dataSize - read);
						int bytes = 0;
						try {
							bytes = file.read(readBuf, toRead);
						} catch (IOException | IllegalArgumentException | KawkabException | InterruptedException e) {
							e.printStackTrace();
							break;
						}
						read += bytes;
					}
					
					double durSec = (System.currentTimeMillis() - startTime)/1000.0;
					double dataMB = (1.0*read)/(1024*1024);
					readStats.putValue(dataMB/durSec);
					System.out.println(String.format("Data size: %.2f, Read Speed: %.2fMB/s", dataMB, dataMB/durSec));
				}
			};
			
			readers[i].start();
		}
		
		for (int i=0; i<readers.length; i++){
			readers[i].join();
		}
		double durSec = (System.currentTimeMillis()-startTime)/1000.0;
		
		System.out.println("Aggregate read throughput (MB/s): " + (dataSize/1024.0/1024*numReaders/durSec));
		System.out.printf("Read stats (MB/s): sum=%.0f %s\n", readStats.sum(), readStats);
	}
	
	private void testConcurrentReadWrite() throws IbmapsFullException, OutOfMemoryException, 
	MaxFileSizeExceededException, InvalidFileOffsetException, InvalidFileModeException, IOException, KawkabException, InterruptedException {
		System.out.println("-----------------------------------------------------------------");
		System.out.println("       Concurrent Read Write Test");
		System.out.println("-----------------------------------------------------------------");
		
		final int numWorkers = 10;
		final int numFiles = 100;
		final int numTasks = 100;
		final int appendSize = 2*Constants.segmentSizeBytes/3;
		final int bufferSize = Constants.dataBlockSizeBytes/2;
		Thread[] workers = new Thread[numWorkers];
		final Random rand = new Random(Constants.thisNodeID);
		final String prefix = "CRWTestL-";
		
		Filesystem.instance().bootstrap();
		
		for(int i=0; i<numWorkers; i++) {
			final int id = i;
			workers[i] = new Thread("Test-worker-"+i) {
				int workerID = id;
				public void run() {
					try {
						Filesystem fs = Filesystem.instance();
						byte[] buffer = new byte[bufferSize];
						
						for (int nTask=0; nTask<numTasks; nTask++) {
							FileHandle file = null;
							String fname = prefix+rand.nextInt(numFiles);
							FileMode mode = rand.nextBoolean() ? FileMode.APPEND : FileMode.READ;
							
							try {
								file = fs.open(fname, mode, new FileOptions());
							} catch (InvalidFileModeException | FileNotExistException e) {
								//System.out.println("\t<"+workerID+"> Skipping file: " + fname);
								nTask--;
								continue;
							}
							
							if (mode == FileMode.APPEND) {
								long sizeMB = (file.size() + appendSize)/1024/1024;
								
								System.out.println("\t<"+workerID+"> Task: " + nTask + ", Writing file: " + fname + " up to " + sizeMB + " MB");
								int appended = 0;
								while(appended < appendSize) {
									int toWrite = (int)(appended+bufferSize <= appendSize ? bufferSize : appendSize - appended);
									rand.nextBytes(buffer);
									appended += file.append(buffer, 0, toWrite);
									//rand.nextBytes(writeBuf);
								}
							} else {
								file.seekBytes(0);
								long dataSize = file.size();
								long read = 0;
								
								System.out.println("\t<"+workerID+"> Task: " + nTask + ", Reading file: " + fname + " up to " + (dataSize/1024/1024) + " MB");
								
								while(read < dataSize) {
									int toRead = (int)(read+bufferSize < dataSize ? bufferSize : dataSize - read);
									int bytes = 0;
									try {
										bytes = file.read(buffer, toRead);
									} catch (IOException | IllegalArgumentException | KawkabException | InterruptedException e) {
										e.printStackTrace();
										break;
									}
									read += bytes;
								}
							}
						}
					} catch (KawkabException | IOException | IbmapsFullException | InterruptedException | 
							OutOfMemoryException | MaxFileSizeExceededException | InvalidFileOffsetException e) {
						e.printStackTrace();
					}
				}
			};
			
			workers[i].start();
		}
		
		for (int i=0; i<numWorkers; i++) {
			workers[i].join();
		}
	}
	
	private void testConcurrentReadWriteLastBlock() throws IbmapsFullException, OutOfMemoryException, 
	MaxFileSizeExceededException, InvalidFileOffsetException, InvalidFileModeException, IOException, KawkabException, InterruptedException {
		System.out.println("-----------------------------------------------------------------");
		System.out.println("       Concurrent Read Write Test");
		System.out.println("-----------------------------------------------------------------");
		
		final int numWorkers = 10;
		final int numFiles = 1;
		final int numTasks = 1000;
		final int appendSize = Constants.segmentSizeBytes/3; // An odd append size to test reading existing block as well as newly created block
		final int bufferSize = Constants.dataBlockSizeBytes/2;
		Thread[] workers = new Thread[numWorkers];
		final Random rand = new Random(Constants.thisNodeID);
		
		Filesystem.instance().bootstrap();
		
		for(int i=0; i<numWorkers; i++) {
			final int id = i;
			workers[i] = new Thread("Test-worker-"+i) {
				int workerID = id;
				public void run() {
					try {
						Filesystem fs = Filesystem.instance();
						byte[] buffer = new byte[bufferSize];
						
						for (int nTask=0; nTask<numTasks; nTask++) {
							String fname = "CRWLBTest-"+rand.nextInt(numFiles);
							FileHandle file = null;
							
							FileMode mode = workerID == 0 ? FileMode.APPEND : FileMode.READ;
							
							try {
								file = fs.open(fname, mode, new FileOptions());
							} catch (InvalidFileModeException | FileNotExistException e) {
								//System.out.println("\t<"+workerID+"> Skipping file: " + fname);
								nTask--;
								continue;
							}
							
							if (mode == FileMode.APPEND) {
								long sizeMB = (file.size() + appendSize)/1024/1024;
								
								System.out.println("\t<"+workerID+"> Task: " + nTask + ", Writing file: " + fname + " up to " + sizeMB + " MB");
								int appended = 0;
								while(appended < appendSize) {
									int toWrite = (int)(appended+bufferSize <= appendSize ? bufferSize : appendSize - appended);
									rand.nextBytes(buffer);
									appended += file.append(buffer, 0, toWrite);
									//rand.nextBytes(writeBuf);
								}
							} else {
								file.seekBytes(file.size()-1);
								long dataSize = 1;
								long read = 0;
								
								System.out.println("\t<"+workerID+"> Task: \" + nTask + \", Reading file: " + fname + " up to " + (dataSize/1024/1024) + " MB");
								
								while(read < dataSize) {
									int toRead = (int)(read+bufferSize < dataSize ? bufferSize : dataSize - read);
									int bytes = 0;
									try {
										bytes = file.read(buffer, toRead);
									} catch (IOException | IllegalArgumentException | KawkabException | InterruptedException e) {
										e.printStackTrace();
										break;
									}
									read += bytes;
								}
							}
						}
					} catch (KawkabException | IOException | IbmapsFullException | InterruptedException | 
							OutOfMemoryException | MaxFileSizeExceededException | InvalidFileOffsetException e) {
						e.printStackTrace();
					}
				}
			};
			
			workers[i].start();
			
			if (i == 0) { //Allow the writer to write some data
				Thread.sleep(1000);
			}
		}
		
		for (int i=0; i<numWorkers; i++) {
			workers[i].join();
		}
	}
	
	/*private void testVeryLargeReadWrite() throws OutOfMemoryException, MaxFileSizeExceededException, 
			IbmapsFullException, InvalidFileOffsetException, InvalidFileModeException, IOException{
		Filesystem fs = Filesystem.instance().bootstrap();
		String filename = new String("/home/smash/testVeryLarge");
		FileOptions opts = new FileOptions();
		FileHandle file = fs.open(filename, FileMode.APPEND, opts);
		
		Random rand = new Random(0);
		int bufSize = 8*1024*1024;
		long dataSize = (long)20*128L*bufSize + 3;
		long appended = 0;
		
		byte[] writeBuf = new byte[bufSize];
		rand.nextBytes(writeBuf);
		
		while(appended < dataSize) {
			int toWrite = (int)(appended+bufSize <= dataSize ? bufSize : dataSize - appended);
			appended += file.append(writeBuf, 0, toWrite);
			rand.nextBytes(writeBuf);
		}
		
		byte[] readBuf = new byte[bufSize];
		rand = new Random(0);

		long read = 0;
		while(read < dataSize) {
			int toRead = (int)(read+bufSize < dataSize ? bufSize : dataSize - read);
			int bytes = file.read(readBuf, toRead);
			read += bytes;
			
			rand.nextBytes(writeBuf);
			if (bytes< writeBuf.length){
				for(int i=0; i<bytes; i++){
					assert readBuf[i] == writeBuf[i];
				}
			} else {
				assert Arrays.equals(readBuf, writeBuf);
			}
		}
		
		assert appended == read;
	}*/
	
	/*public void testFileSeek() throws OutOfMemoryException, MaxFileSizeExceededException, InterruptedException{
		Filesystem fs = Filesystem.instance();
		String filename = new String("/home/smash/testFileSeek");
		FileOptions opts = new FileOptions();
		FileHandle file = fs.open(filename, FileMode.APPEND, opts);
		
		//When no block is available
		assert file.seekAfterTime(0) == null;
		
		Random rand = new Random(0);
		byte[] block1 = new byte[Filesystem.BlockSize()];
		byte[] block2 = new byte[Filesystem.BlockSize()];
		byte[] block3 = new byte[Filesystem.BlockSize()];
		byte[] block4 = new byte[Filesystem.BlockSize()];
		
		long t1 = System.currentTimeMillis();
		rand.nextBytes(block1);
		file.append(block1, 0, block1.length);
		
		Thread.sleep(100);
		long t2 = System.currentTimeMillis();
		rand.nextBytes(block2);
		file.append(block2, 0, block2.length);
		
		Thread.sleep(100);
		long t3 = System.currentTimeMillis();
		rand.nextBytes(block3);
		file.append(block3, 0, block3.length);
		
		Thread.sleep(100);
		long t4 = System.currentTimeMillis();
		rand.nextBytes(block4);
		file.append(block4, 0, block4.length);
		
		byte[] data = new byte[Filesystem.BlockSize()];
		
		//First block, regardless of the time.
		file.seekAfterTime(0);
		file.read(data, data.length);
		assert Arrays.equals(data, block1);
		
		//Invalid block
		assert file.seekBeforeTime(0) == null;
		
		//Invalid block
		assert file.seekAfterTime(t4+10000) == null;
		
		//Last block regardless of the time.
		file.seekBeforeTime(t4+10000);
		file.read(data, data.length);
		assert Arrays.equals(data, block4);
		
		//Block before time when no block contains the given time
		file.seekBeforeTime(t2 - 10);
		file.read(data, data.length);
		assert Arrays.equals(data, block1);
		
		//Block after time when no block contains the given time
		file.seekAfterTime(t2 - 10);
		file.read(data, data.length);
		assert Arrays.equals(data, block2);
		
		//Left block containing time
		file.seekAfterTime(t2);
		file.read(data, data.length);
		assert Arrays.equals(data, block2);
		
		//Right block containing time
		file.seekAfterTime(t3);
		file.read(data, data.length);
		assert Arrays.equals(data, block3);
	}*/
	
	private void testShutdown() throws KawkabException, InterruptedException, IOException{
		System.out.println("--------------------------------------------");
		System.out.println("            Filesystem shutdown Test");
		System.out.println("--------------------------------------------");
		
		Filesystem.instance().shutdown();
	}
}
