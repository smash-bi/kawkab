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
import kawkab.fs.core.exceptions.FileNotExistException;
import kawkab.fs.core.exceptions.KawkabException;

public class LocalProcessor {
	//private ExecutorService workers;
	private GlobalProcessor globalProc;
	
	private final long maxBlocks = Constants.maxBlocksPerLocalDevice;
	private long usedBlocks;
	private Lock lock;
	private LinkedBlockingQueue<Block> reqQs[];
	private Thread[] workers;
	private final int numWorkers;
	private volatile boolean working = true;
	
	private LocalStoreDB storedFilesMap;
	
	private static LocalProcessor instance;
	
	private LocalProcessor() {
		//workers = Executors.newFixedThreadPool(numWorkers);
		globalProc = S3Backend.instance();
		lock = new ReentrantLock();
		this.numWorkers = Constants.syncThreadsPerDevice;
		
		try {
			storedFilesMap = new LocalStoreDB();
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}
		
		reqQs = new LinkedBlockingQueue[numWorkers];
		for(int i=0; i<numWorkers; i++) {
			reqQs[i] = new LinkedBlockingQueue<Block>();
		}
		
		startWorkers();
	}
	
	public static LocalProcessor instance() {
		if (instance == null) {
			instance = new LocalProcessor();
		}
		
		return instance;
	}
	
	public void store(Block block) throws KawkabException {
		if (!working) {
			throw new KawkabException("LocalProcessor has already received stop signal.");
		}
		
		//Load balance between workers, but assign same worker to the same block.
		int queueNum = block.id().key().hashCode() % numWorkers;
		reqQs[queueNum].add(block);
	}
	
	public void load(Block block) throws FileNotExistException,KawkabException {
		BlockID id = block.id();
		System.out.println("[LS] Load block: " + id.name());
		
		if (!storedFilesMap.exists(id)) {
			//Get the block from the global store.
			globalProc.load(block);
			
			System.out.println("Loaded block from the global store: " + id.name());
			//storedFilesMap.put(block);
		}
		
		try {
			loadBlock(block);
		} catch (IOException e) {
			throw new KawkabException(e);
		}
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
		System.out.println("[LS] Store block: " + block.id().name());
		int dirtyCount = block.dirtyCount();
		
		if (dirtyCount == 0)
			return;
		
		try {
			storeBlock(block);
			
			storedFilesMap.put(block.id()); //FIXME: What if an error occurs here. Should we remove the locally stored block?
			
			if (block.shouldStoreGlobally()) {
				globalProc.store(block);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (KawkabException e) {
			e.printStackTrace();
		}
		
		block.clearDirty(dirtyCount);
	}
	
	private void loadBlock(Block block) throws IOException {
		try(RandomAccessFile file = 
                new RandomAccessFile(block.id().localPath(), "r")) {
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
		
		//useBlock(); //FIXME: Limit the number of files created in the local storage.
		
		try(RandomAccessFile rwFile = 
                new RandomAccessFile(block.id().localPath(), "rw")) {
			try (SeekableByteChannel channel = rwFile.getChannel()) {
				channel.position(block.appendOffsetInBlock());
				//System.out.println("Store: "+block.id() + ": " + channel.position());
				block.storeTo(channel);
			}
		}
	}
	
	public void storeLocally(Block block) throws IOException {
		File file = new File(block.id().localPath());
		File parent = file.getParentFile();
		if (!parent.exists()){
			parent.mkdirs();
		}
		
		storeBlock(block);
		storedFilesMap.put(block.id());
	}
	
	public void stop() {
		System.out.println("Closing LocalProcessor...");
		
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

	public boolean exists(BlockID id) {
		return storedFilesMap.exists(id);
	}
}
