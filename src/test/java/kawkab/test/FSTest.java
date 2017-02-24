package kawkab.test;

import java.util.Arrays;
import java.util.Random;

import kawkab.fs.api.FileHandle;
import kawkab.fs.api.FileOptions;
import kawkab.fs.core.Filesystem;
import kawkab.fs.core.Filesystem.FileMode;
import kawkab.fs.core.exceptions.MaxFileSizeExceededException;
import kawkab.fs.core.exceptions.OutOfMemoryException;

public class FSTest {
	public static void main(String args[]) throws OutOfMemoryException, MaxFileSizeExceededException {
		FSTest tester = new FSTest();
		//tester.testSmallReadWrite();
		//tester.testLargeReadWrite();
		tester.testVeryLargeReadWrite();
	}
	
	public void testSmallReadWrite() throws OutOfMemoryException, MaxFileSizeExceededException{
		Filesystem fs = Filesystem.instance();
		
		String filename = new String("/home/smash/testSmall");
		FileOptions opts = new FileOptions();
		
		FileHandle file = fs.open(filename, FileMode.APPEND, opts);
		
		byte[] dataBuffer = new byte[100];
		new Random().nextBytes(dataBuffer);
		
		int appended = file.append(dataBuffer, 0, dataBuffer.length);
		
		byte[] readBuf = new byte[dataBuffer.length];
		int read = file.read(readBuf, readBuf.length);

		assert appended == read;
		assert Arrays.equals(dataBuffer, readBuf);
	}
	
	public void testLargeReadWrite() throws OutOfMemoryException, MaxFileSizeExceededException{
		Filesystem fs = Filesystem.instance();
		
		String filename = new String("/home/smash/testLarge");
		FileOptions opts = new FileOptions();
		FileHandle file = fs.open(filename, FileMode.APPEND, opts);
		
		int dataSize = 1*1024*1024*1024;
		byte[] dataBuffer = new byte[dataSize];
		new Random().nextBytes(dataBuffer);
		
		int appended = file.append(dataBuffer, 0, dataBuffer.length);
		
		byte[] readBuf = new byte[dataBuffer.length];
		int read = file.read(readBuf, readBuf.length);
		
		//System.out.println(Arrays.toString(dataBuffer));
		//System.out.println(Arrays.toString(readBuf));
		
		assert appended == read;
		assert Arrays.equals(dataBuffer, readBuf);
	}
	
	public void testVeryLargeReadWrite() throws OutOfMemoryException, MaxFileSizeExceededException{
		Filesystem fs = Filesystem.instance();
		
		String filename = new String("/home/smash/testVeryLarge");
		FileOptions opts = new FileOptions();
		FileHandle file = fs.open(filename, FileMode.APPEND, opts);
		
		Random rand = new Random(0);
		int bufSize = 128*1024*1024;
		long dataSize = (long)512*bufSize;
		long appended = 0;
		long read = 0;
		
		byte[] writeBuf = new byte[bufSize];
		rand.nextBytes(writeBuf);
		
		while(appended < dataSize) {
			int toWrite = (int)(appended+bufSize <= dataSize ? bufSize : dataSize - appended);
			
			appended += file.append(writeBuf, 0, toWrite);
		}
		
		byte[] readBuf = new byte[bufSize];
		rand = new Random(0);
		rand.nextBytes(writeBuf);

		while(read < dataSize) {
			int toRead = (int)(read+bufSize < dataSize ? bufSize : dataSize - read);
			read += file.read(readBuf, toRead);
			
			rand.nextBytes(writeBuf);
			
			assert Arrays.equals(readBuf, writeBuf);
		}
		
		assert appended == read;
	}
}
