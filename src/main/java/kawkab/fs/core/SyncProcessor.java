package kawkab.fs.core;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class SyncProcessor {
	private ExecutorService workers;
	
	public SyncProcessor(int numWorkers) {
		workers = Executors.newFixedThreadPool(numWorkers);
	}
	
	public void store(Block block) {
		workers.submit(() -> { runWorker(block); });
	}
	
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
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		block.clearDirty(dirtyCount);
	}
	
	private void loadBlock(Block block) throws IOException {
		try(RandomAccessFile file = 
                new RandomAccessFile(block.localPath(), "r")) {
			try (FileChannel channel = file.getChannel()) {
				block.loadFrom(channel);
			}
		}
	}
	
	private void storeBlock(Block block) throws IOException {
		//FIXME: This creates a new file if it does not already exist. We should prevent that in order to make sure that
		//first we create a file and do proper accounting for the file.
		
		try(RandomAccessFile rwFile = 
                new RandomAccessFile(block.localPath(), "rw")) {
			try (FileChannel channel = rwFile.getChannel()) {
				block.storeTo(channel);
			}
		}
	}
	
	public void storeSynced(Block block) throws IOException {
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
}
