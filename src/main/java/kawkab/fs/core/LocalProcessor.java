package kawkab.fs.core;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.SeekableByteChannel;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import kawkab.fs.commons.Constants;

public class LocalProcessor implements SyncProcessor {
	//private ExecutorService workers;
	private SyncProcessor globalProc;
	
	private final long maxBlocks = Constants.maxBlocksPerLocalDevice;
	private long usedBlocks;
	private Lock lock;
	private LinkedBlockingQueue<Block> reqQs[];
	private Thread[] workers;
	private final int numWorkers;
	private volatile boolean working = true;
	
	public LocalProcessor(int numWorkers) {
		//workers = Executors.newFixedThreadPool(numWorkers);
		globalProc = new GlobalProcessor();
		lock = new ReentrantLock();
		this.numWorkers = numWorkers;
		
		reqQs = new LinkedBlockingQueue[numWorkers];
		for(int i=0; i<numWorkers; i++) {
			reqQs[i] = new LinkedBlockingQueue<Block>();
		}
		
		startWorkers();
	}
	
	@Override
	public void store(Block block) throws IOException {
		if (!working) {
			throw new IOException("LocalProcessor has already received stop signal. It is not accepting new requests");
		}
		
		int queueNum = (int)(block.id.highBits ^ block.id.lowBits) % numWorkers; //Want to assign blocks from different files to different workers
		reqQs[queueNum].add(block);
	}
	
	@Override
	public void load(Block block) throws IOException {
		loadBlock(block);
	}
	
	private void startWorkers() {
		workers = new Thread[numWorkers];
		for (int i=0; i<workers.length; i++) {
			final int workerID = i;
			workers[i] = new Thread() {
				public void run() {
					runWorker(reqQs[workerID]);
				}
			};
			
			workers[i].start();
		}
	}
	
	private void runWorker(LinkedBlockingQueue<Block> reqs) {
		while(working) {
			Block block = null;
			try {
				block = reqs.poll(1, TimeUnit.SECONDS);
			} catch (InterruptedException e1) {
				if (!working) {
					break;
				}
			}
		
			if (block == null)
				continue;
			
			processStoreRequest(block);
		}
		
		Block block = null;
		while( (block = reqs.poll()) != null) {
			processStoreRequest(block);
		}
	}
	
	private void processStoreRequest(Block block) {
		int dirtyCount = block.dirtyCount();
		
		if (dirtyCount == 0)
			return;
		
		try {
			storeBlock(block);
			
			globalProc.store(block);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		block.clearDirty(dirtyCount);
	}
	
	private void loadBlock(Block block) throws IOException {
		try(RandomAccessFile file = 
                new RandomAccessFile(block.localPath(), "r")) {
			try (SeekableByteChannel channel = file.getChannel()) {
				channel.position(block.appendOffsetInBlock());
				//System.out.println("Load: "+block.localPath() + ": " + channel.position());
				block.loadFrom(channel);
			}
		}
	}
	
	private void storeBlock(Block block) throws IOException {
		//FIXME: This creates a new file if it does not already exist. We should prevent that in order to make sure that
		//first we create a file and do proper accounting for the file.
		
		//useBlock();
		
		try(RandomAccessFile rwFile = 
                new RandomAccessFile(block.localPath(), "rw")) {
			try (SeekableByteChannel channel = rwFile.getChannel()) {
				channel.position(block.appendOffsetInBlock());
				//System.out.println("Store: "+block.id() + ": " + channel.position());
				block.storeTo(channel);
			}
		}
	}
	
	public void storeLocally(Block block) throws IOException {
		File file = new File(block.localPath());
		File parent = file.getParentFile();
		if (!parent.exists()){
			parent.mkdirs();
		}
		
		storeBlock(block);
	}
	
	public void stop() {
		System.out.println("Closing syncProc...");
		
		working = false;
		
		if (workers == null)
			return;
		
		for (int i=0; i<numWorkers; i++) {
			//workers[i].interrupt();
			try {
				workers[i].join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	private void useBlock() { //Limit the number of blocks stored in the local store
		lock.lock();
		
		if (usedBlocks == maxBlocks) {
			evictBlocks();
		}
		
		usedBlocks += 1;
		
		lock.unlock();
	}
	
	private void evictBlocks() {
		usedBlocks -= 1;
	}
}
