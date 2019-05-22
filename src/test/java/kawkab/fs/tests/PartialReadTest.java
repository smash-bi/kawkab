package kawkab.fs.tests;

import kawkab.fs.api.FileOptions;
import kawkab.fs.core.Cache;
import kawkab.fs.core.FileHandle;
import kawkab.fs.core.Filesystem;
import kawkab.fs.core.Filesystem.FileMode;
import kawkab.fs.core.exceptions.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

public class PartialReadTest {

	@Before
	public void initialize() throws IOException, InterruptedException, KawkabException, AlreadyConfiguredException {
		int nodeID = getNodeID();
		Properties props = getProperties();
		
		Filesystem.bootstrap(nodeID, props);
	}
	
	@After
	public void terminate() throws KawkabException, InterruptedException, IOException {
		Filesystem.instance().shutdown();
	}
	
	@Test
	public void partialReadSameThreadTest()
			throws IbmapsFullException, OutOfMemoryException, MaxFileSizeExceededException, InvalidFileOffsetException,
			IOException, KawkabException, InterruptedException, FileAlreadyOpenedException {
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
		Cache.instance().flush();

		System.out.println("Writing after flush");

		file = fs.open(filename, FileMode.APPEND, opts);

		byte[] afterFlushData = randomData(13);
		rand.nextBytes(afterFlushData);
		file.append(afterFlushData, 0, afterFlushData.length);

		byte[] readData = new byte[beforeFlushData.length];
		byte[] allData = new byte[beforeFlushData.length + afterFlushData.length];

		file.read(readData, initialOffset, beforeFlushData.length);
		file.read(allData, initialOffset, allData.length);

		/*System.out.println(Arrays.toString(beforeFlushData));
		System.out.println(Arrays.toString(readData));
		System.out.println(Arrays.toString(afterFlushData));
		System.out.println(Arrays.toString(allData));*/

		assertArrayEquals(beforeFlushData, readData);

		fs.close(file);
	}
	
	private Properties getProperties() throws IOException {
		String propsFile = "/config.properties";
		
		try (InputStream in = Thread.class.getResourceAsStream(propsFile)) {
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
