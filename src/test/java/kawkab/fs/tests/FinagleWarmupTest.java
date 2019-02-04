package kawkab.fs.tests;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import kawkab.fs.client.services.finagle.FFilesystemServiceClient;
import kawkab.fs.commons.Constants;
import kawkab.fs.core.Filesystem.FileMode;
import kawkab.fs.core.NodeInfo;
import kawkab.fs.core.exceptions.KawkabException;

/**
 * Warms up the filesystem using the Finagle RPC
 */
public class FinagleWarmupTest {
	public static void main(String[] args) throws KawkabException, IOException, InterruptedException {
		FinagleWarmupTest ft = new FinagleWarmupTest();
		ft.warmupTest();
	}
	
	@Test
	public void warmupTest() {
		System.out.println("Warming up...");
		
		final int filesPerNode = 2;
		final long fileSizeBytes = 100 * 1024 * 1024;
		final int bufLen = 4 * 1024;
		final int numThreads = 12;
		
		Random rand = new Random();
		ExecutorService executor = Executors.newFixedThreadPool(numThreads);
		
		BlockingQueue<String> files = new LinkedBlockingQueue<String>();
		int filesCount = 0;
		
		Map<Integer, NodeInfo> nodesMap = Constants.nodesMap;
		for (int i=0; i<nodesMap.size(); i++) {
			final String address = String.format("%s:%d",nodesMap.get(i).ip, Constants.FS_SERVER_LISTEN_PORT);
			for (int iFile=0; iFile<filesPerNode; iFile++) {
				filesCount++;
				final int clID = filesCount;
				final int nodeid = i;
				
				executor.execute(new Runnable() {
					final int id = clID;
					final int nodeID = nodeid;
					@Override
					public void run() {
						String filename = String.format("wm-%d-%d-%d", nodeID, id, rand.nextInt(100000));
						System.out.printf("[%d] Creating file: %s, server=%s\n", id, filename, address);
						write(address, filename, bufLen, fileSizeBytes);
						System.out.printf("[%d] Done: %s\n",id, filename);
						
						files.add(filename);
					}
				});
			}
		}
		
		while(filesCount-- > 0) {
			final String filename;
			try {
				filename = files.take();
			} catch (InterruptedException e) {
				e.printStackTrace();
				continue;
			}
			
			executor.submit(new Runnable() {
				String fname = filename;
				@Override
				public void run() {
					final String address = String.format("%s:%d",nodesMap.get(rand.nextInt(nodesMap.size())).ip, Constants.FS_SERVER_LISTEN_PORT);
					System.out.printf("Reading file: %s from %s\n", fname, address);
					read(address, fname, bufLen, fileSizeBytes);
					System.out.printf("Completed reading: %s from %s\n", fname, address);
				}
				
			});
		}
		
		executor.shutdown();
		try {
			executor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		System.out.println("Finished warmup");
	}
	
	private void read(String address, String filename, int bufLen, long size) {
		FFilesystemServiceClient client = new FFilesystemServiceClient(address);
		try {
			long sessionID = client.open(filename, FileMode.READ);
			
			int rcvd = 0;
			while(rcvd < size) {
				long remaining = size-rcvd;
				int toRead = (int)(remaining >= bufLen ? bufLen : remaining);
				
				ByteBuffer buffer = client.read(sessionID, rcvd, toRead);
				rcvd += buffer.remaining();
			}
		} catch (KawkabException e) {
			e.printStackTrace();
		}
	}
	
	private void write (String address, String filename, int bufLen, long size) {
		Random rand = new Random();
		byte[] bytes = new byte[bufLen];
		FFilesystemServiceClient client = new FFilesystemServiceClient(address);
		try {
			long sessionID = client.open(filename, FileMode.APPEND);
			
			int sent = 0;
			while(sent < size) {
				long remaining = size-sent;
				int toSend = (int)(remaining >= bufLen ? bufLen : remaining);
				rand.nextBytes(bytes);
				ByteBuffer buffer = ByteBuffer.wrap(bytes, 0, toSend);
				buffer.rewind();
				
				int appended = client.append(sessionID, buffer);
				sent += appended;
			}
		} catch (KawkabException e) {
			e.printStackTrace();
			
		}
	}
}
