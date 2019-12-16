package kawkab.fs.tests;

import kawkab.fs.api.FileOptions;
import kawkab.fs.commons.Configuration;
import kawkab.fs.core.Cache;
import kawkab.fs.core.FileHandle;
import kawkab.fs.core.Filesystem;
import kawkab.fs.core.Filesystem.FileMode;
import kawkab.fs.core.exceptions.KawkabException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Properties;
import java.util.Random;

public class PartialReadTest {

	@BeforeAll
	public static void initialize() throws IOException, InterruptedException, KawkabException {
		int nodeID = Configuration.getNodeID();
		Properties props = Configuration.getProperties(Configuration.propsFileCluster);
		
		System.out.println("Node ID = " + nodeID);
		System.out.println("Loading properties from: " + props);
		
		Filesystem.bootstrap(nodeID, props);
	}
	
	@AfterAll
	public static void terminate() throws KawkabException, InterruptedException, IOException {
		Filesystem.instance().shutdown();
	}
	
	@Test
	public void partialReadSameThreadTest() throws IOException, KawkabException, InterruptedException {
		System.out.println("----------------------------------------------------------------");
		System.out.println("            Partial Read Test - Same Thread");
		System.out.println("----------------------------------------------------------------");

		Filesystem fs = Filesystem.instance();

		String filename = "/home/smash/partialReadTest-";
		FileOptions opts = new FileOptions();

		System.out.println("Opening file: " + filename);

		FileHandle file = fs.open(filename, FileMode.APPEND, opts);

		byte[] beforeFlushData = randomData(13);
		Random rand = new Random();

		long initialOffset = file.size();

		System.out.println("Writing before flush");
		// Fill some bytes in a segment
		rand.nextBytes(beforeFlushData);
		file.append(beforeFlushData, 0, beforeFlushData.length);
		// Release the inodesBlock for cache flush
		fs.close(file);
		// Empty the cache so that the data is partially loaded for append next time
		fs.flush();

		System.out.println("Writing after flush");

		file = fs.open(filename, FileMode.APPEND, opts);

		byte[] afterFlushData = randomData(13);
		rand.nextBytes(afterFlushData);
		file.append(afterFlushData, 0, afterFlushData.length);

		byte[] readData = new byte[beforeFlushData.length];
		byte[] allData = new byte[beforeFlushData.length + afterFlushData.length];

		file.read(readData, initialOffset, beforeFlushData.length, true);
		file.read(allData, initialOffset, allData.length, true);

		/*System.out.println(Arrays.toString(beforeFlushData));
		System.out.println(Arrays.toString(readData));
		System.out.println(Arrays.toString(afterFlushData));
		System.out.println(Arrays.toString(allData));*/

		Assertions.assertArrayEquals(beforeFlushData, readData);

		fs.close(file);
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
