package kawkab.test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import kawkab.fs.api.FileHandle;
import kawkab.fs.api.FileOptions;
import kawkab.fs.commons.Constants;
import kawkab.fs.core.Filesystem;
import kawkab.fs.core.Filesystem.FileMode;
import kawkab.fs.core.exceptions.IbmapsFullException;
import kawkab.fs.core.exceptions.InvalidFileModeException;
import kawkab.fs.core.exceptions.InvalidFileOffsetException;
import kawkab.fs.core.exceptions.MaxFileSizeExceededException;
import kawkab.fs.core.exceptions.OutOfMemoryException;

public class FSTest {
	public static void main(String args[]) throws OutOfMemoryException, 
				MaxFileSizeExceededException, InterruptedException, IbmapsFullException, 
				InvalidFileOffsetException, InvalidFileModeException {
		//FSTest tester = new FSTest();
		//tester.testBlocksCreation();
		//tester.testSmallReadWrite();
		//tester.testLargeReadWrite();
		//tester.testVeryLargeReadWrite();
		//tester.testFileSeek();
	}
	
	public void testConfiguration(){
		Constants.printConfig();
	}
	
	public void testBootstrap() throws IOException{
		Filesystem fs = Filesystem.instance();
		fs.bootstrap();
	}
	
	public void testBlocksCreation() throws IbmapsFullException, OutOfMemoryException, 
				MaxFileSizeExceededException, InvalidFileOffsetException, InvalidFileModeException, IOException{
		Filesystem fs = Filesystem.instance().bootstrap();
		String filename = "testFile";
		FileHandle file = fs.open(filename, FileMode.APPEND, new FileOptions());
		
		byte[] data = new byte[10];
		file.append(data, 0, data.length);
	}
	
	public void testSmallReadWrite() throws OutOfMemoryException, MaxFileSizeExceededException, IbmapsFullException, 
				InvalidFileOffsetException, InvalidFileModeException, IOException{
		Filesystem fs = Filesystem.instance().bootstrap();
		
		String filename = new String("/home/smash/testSmall");
		FileOptions opts = new FileOptions();
		
		FileHandle file = fs.open(filename, FileMode.APPEND, opts);
		
		byte[] dataBuffer = new byte[100];
		new Random().nextBytes(dataBuffer);
		
		file.seekBytes(file.size());
		int appended = file.append(dataBuffer, 0, dataBuffer.length);
		
		byte[] readBuf = new byte[dataBuffer.length];
		int read = file.read(readBuf, readBuf.length);

		assert appended == read;
		assert Arrays.equals(dataBuffer, readBuf);
	}
	
	public void testLargeReadWrite() throws OutOfMemoryException, MaxFileSizeExceededException, IbmapsFullException, 
				InvalidFileOffsetException, InvalidFileModeException, IOException{
		Filesystem fs = Filesystem.instance().bootstrap();
		
		String filename = new String("/home/smash/testLarge");
		FileOptions opts = new FileOptions();
		FileHandle file = fs.open(filename, FileMode.APPEND, opts);
		
		int dataSize = 1024*1024; //1*1024*1024*1024;
		byte[] dataBuffer = new byte[dataSize];
		new Random().nextBytes(dataBuffer);
		
		file.seekBytes(file.size());
		int appended = file.append(dataBuffer, 0, dataBuffer.length);
		
		byte[] readBuf = new byte[dataBuffer.length];
		int read = file.read(readBuf, readBuf.length);
		
		//System.out.println(Arrays.toString(dataBuffer));
		//System.out.println(Arrays.toString(readBuf));
		
		assert appended == read;
		assert Arrays.equals(dataBuffer, readBuf);
	}
	
	/*public void testVeryLargeReadWrite() throws OutOfMemoryException, MaxFileSizeExceededException, 
			IbmapsFullException, InvalidFileOffsetException, InvalidFileModeException, IOException{
		Filesystem fs = Filesystem.instance().bootstrap();
		String filename = new String("/home/smash/testVeryLarge");
		FileOptions opts = new FileOptions();
		FileHandle file = fs.open(filename, FileMode.APPEND, opts);
		
		Random rand = new Random(0);
		int bufSize = 8*1024*1024;
		long dataSize = (long)20*128*bufSize + 3;
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
	
	void testShutdown(){
		Filesystem.shutdown();
	}
}
