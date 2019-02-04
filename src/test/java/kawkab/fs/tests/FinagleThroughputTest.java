package kawkab.fs.tests;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.locks.LockSupport;

import org.junit.Test;

import kawkab.fs.client.services.finagle.FFilesystemServiceClient;
import kawkab.fs.commons.Constants;
import kawkab.fs.commons.Stats;
import kawkab.fs.core.Filesystem.FileMode;
import kawkab.fs.core.exceptions.KawkabException;

/**
 * Tests the throughput of the filesystem
 */
public class FinagleThroughputTest {
	public static void main(String[] args) throws KawkabException, IOException, InterruptedException {
		FinagleThroughputTest ft = new FinagleThroughputTest();
		ft.throughputTest();
	}
	
	
	@Test
	public void throughputTest() throws KawkabException {
		final int numClients = 30;
		final int numNodes = 3; // Number of servers to connect in a round-robin fashion
		final int appendSize = 100 * 1024 * 1024; 
		final int bufLen = 1 * 1024;
		
		final Stats latStats = new Stats();
		final Stats thrStats = new Stats();
		final Stats opsThrStats = new Stats();
		final DoubleAdder tThr = new DoubleAdder();
		final DoubleAdder tOpsThr = new DoubleAdder();
		
		ExecutorService executor = Executors.newFixedThreadPool(numClients);
		Random rand = ThreadLocalRandom.current();
		
		assert numNodes > 0 && numNodes <= Constants.nodesMap.size();
		
		for (int i=0; i<numClients; i++) {
			final int clid = i;
			
			executor.submit(new Runnable() {
				int id = clid;
				@Override
				public void run() {
					ThreadLocalRandom rand = ThreadLocalRandom.current();
					int svrIdx = id % numNodes; ;
					String svrIP = Constants.nodesMap.get(svrIdx).ip;
					String svrAddr = String.format("%s:%d",svrIP,Constants.FS_SERVER_LISTEN_PORT);
					String fname = String.format("ft-%d-%d", id, rand.nextInt(1000000));
					System.out.printf("[%d] Writing to file: %s on %s\n",id,fname,svrIP);
					FFilesystemServiceClient client = new FFilesystemServiceClient(svrAddr);
					
					try {
						byte[] data = new byte[bufLen];
						rand.nextBytes(data);
						ByteBuffer buffer = ByteBuffer.wrap(data);
						
						long sessionID = client.open(fname, FileMode.APPEND);
						
						Stats opLatStats = new Stats();
						int sent = 0;
						long ops = 0;
						
						long sTime = System.currentTimeMillis();
						long prevTime = System.nanoTime();
						long now;
						int remaining;
						int toSend;
						while(sent < appendSize) {
							remaining = appendSize - sent;
							toSend = remaining > bufLen ? bufLen : remaining;
							buffer.clear();
							buffer.limit(toSend);
							
							sent += client.append(sessionID, buffer);
							
							now = System.nanoTime();
							opLatStats.putValue((now - prevTime)/1000.0);
							
							ops++;
							prevTime = now;
						}
						
						double elapsedMs = (System.currentTimeMillis() - sTime);
						double thr = (appendSize/(1024.0*1024.0)) / (elapsedMs/1000) ; //MB per second
						double opsThr = ops/(elapsedMs/1000); // Ops pser second
						latStats.putValue(opLatStats.mean());
						thrStats.putValue(thr);
						opsThrStats.putValue(opsThr);
						
						tThr.add(thr);
						tOpsThr.add(opsThr);
						
						client.close(sessionID);
						
						System.out.printf("[%d] Elapsed (msec) = %.0f, Thr (MB/s) = %.2f, Ops per sec = %.2f, Latenncy stats (us): %s\n",id, elapsedMs, thr, opsThr, opLatStats);
					} catch (Exception e) {
						e.printStackTrace();
						return;
					}
				}
			});
			
			LockSupport.parkNanos(10000+rand.nextInt(30)); //Wait between 10 and 40 microseconds 
		}
		
		executor.shutdown();
		try {
			executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		System.out.printf("\nLatency stats (us): %s\n",latStats);
		System.out.printf("Throughput MB/s: %s\n", thrStats);
		System.out.printf("Ops per second: %s\n", opsThrStats);
		System.out.printf("Clinets = %d, thr = %.2f MB/s, ops/sec = %.2f\n", numClients, tThr.doubleValue(), tOpsThr.doubleValue());
		System.out.printf("%d\t%.0f\t%.0f\t%.0f\n", numClients,tThr.doubleValue(), latStats.mean(), tOpsThr.doubleValue());
	}
}
