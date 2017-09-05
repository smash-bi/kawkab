package kawkab.fs.core;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.SeekableByteChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import kawkab.fs.commons.Constants;

public class LocalProcessor implements SyncProcessor {
	private ExecutorService workers;
	private SyncProcessor globalProc;
	
	private final long maxBlocks = Constants.maxBlocksPerLocalDevice;
	private long usedBlocks;
	private Lock lock;
	
	public LocalProcessor(int numWorkers) {
		workers = Executors.newFixedThreadPool(numWorkers);
		globalProc = new GlobalProcessor();
		lock = new ReentrantLock();
	}
	
	@Override
	public void store(Block block) throws IOException {
		//FIXME: Assign same worker to the same block. Otherwise two or more workers can write same data
		//to the same file concurrently and then the dirtyCount will be wrong.
		workers.submit(() -> { runWorker(block); });
	}
	
	@Override
	public void load(Block block) throws IOException {
		loadBlock(block);
	}
	
	private void runWorker(Block block) {
		if (block == null)
			return;
		
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
		
		useBlock();
		
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
		
		assert workers != null;
		
		workers.shutdown();
		try {
			while (!workers.awaitTermination(2, TimeUnit.SECONDS)) { //Wait until all blocks have been written to disk.
				System.err.println("Unable to close all sync threads.");
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			workers.shutdownNow();
		}
	}
	
	private void useBlock() {
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
