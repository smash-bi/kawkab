package kawkab.fs.tests;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Random;

import org.junit.Test;

import kawkab.fs.client.services.finagle.FFilesystemServiceClient;
import kawkab.fs.commons.Constants;
import kawkab.fs.commons.Stats;
import kawkab.fs.core.Filesystem.FileMode;
import kawkab.fs.core.NodeInfo;
import kawkab.fs.core.exceptions.KawkabException;

/**
 * Tests the capture to process latency of the filesystem. Finagle RPC is used in this test.
 */
public class FinagleReadTest {
	public static void main(String[] args) throws KawkabException, IOException, InterruptedException {
		FinagleReadTest ft = new FinagleReadTest();
		ft.immediateReadTest();
	}
	
	
	@Test
	public void immediateReadTest() throws KawkabException {
		Stats readStats = new Stats();
		Stats apndStats = new Stats();
		Stats srvrStats = new Stats();
		int tries = 20;
		
		for (int i=0; i<tries; i++) {
			Map<Integer, NodeInfo> nodesMap = Constants.nodesMap;
			Random rand = new Random();
			int rnum = rand.nextInt(nodesMap.size());
			String primary = nodesMap.get(rnum).ip;
			String secondary = nodesMap.get((rnum+1)%nodesMap.size()).ip;
			
			String primaryAddr = String.format("%s:%d",primary,Constants.FS_SERVER_LISTEN_PORT);
			String secondaryAddr = String.format("%s:%d",secondary,Constants.FS_SERVER_LISTEN_PORT);
			
			FFilesystemServiceClient apndClient = new FFilesystemServiceClient(primaryAddr);
			FFilesystemServiceClient readClient = new FFilesystemServiceClient(secondaryAddr);
			
			String filename = "irt-"+rand.nextInt(100000);
			
			long apndSession = apndClient.open(filename, FileMode.APPEND);
			long readSession = readClient.open(filename, FileMode.READ);
			
			int apndSize = Math.min(2*1024, Constants.segmentSizeBytes/2);
			
			byte[] data = new byte[apndSize];
			rand.nextBytes(data);
			
			ByteBuffer buffer = ByteBuffer.wrap(data);
			buffer.rewind();
			
			System.out.printf("File %s, writing to %s, reading from %s\n", filename, primary, secondary);
			
			long sTime = System.currentTimeMillis();
			apndClient.append(apndSession, buffer);
			long aTime = System.currentTimeMillis();
			ByteBuffer retBuf = readClient.read(readSession, 0, apndSize);
			long eTime = System.currentTimeMillis();
			
			long elapsedOnServer = retBuf.getLong();
			
			apndStats.putValue(aTime-sTime);
			readStats.putValue(eTime-aTime);
			srvrStats.putValue(elapsedOnServer);
			
			
			apndClient.close(apndSession);
			readClient.close(readSession);
			
			System.out.printf("%d. append (ms) = %d, read (ms) = %d, lat on server = %d\n", i+1, aTime-sTime, eTime-aTime, elapsedOnServer);
		}
		
		System.out.println("Append stats (ms): " + apndStats);
		System.out.println("Capture to process latency stats (ms): " + readStats);
		System.out.println("Read latency on the server (ms): " + srvrStats);
	}
}
