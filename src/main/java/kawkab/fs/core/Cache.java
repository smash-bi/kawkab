package kawkab.fs.core;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;

import kawkab.fs.commons.Constants;

public class Cache extends LinkedHashMap<String, Block> {
	private static Cache instance;
	//private Map<String, Block> cache;
	private LocalStore store;
	private LinkedBlockingQueue<Block> dirtyBlocks;
	private Thread blocksWriter;
	private volatile boolean stop;
	
	private Cache(){
		super(Constants.maxBlocksInCache+1, 1.1f, true);
		store = LocalStore.instance();
		dirtyBlocks = new LinkedBlockingQueue<Block>();
		runBlocksWriter();
	}
	
	public static Cache instance(){
		if (instance == null) {
			instance = new Cache();
		}
		
		return instance;
	}
	
	public InodesBlock getInodesBlock(int blockNumber){
		InodesBlock block = (InodesBlock)getFromCache(InodesBlock.name(blockNumber));
		
		if (block == null){
			try {
				block = new InodesBlock(blockNumber);
				store.readBlock(block);
				putInCache(block.name(), block);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		return block;
	}
	
	Ibmap getIbmap(int blockIndex){
		Ibmap block = (Ibmap)getFromCache(Ibmap.name(blockIndex));
		if (block == null){
			try {
				block = new Ibmap(blockIndex);
				store.readBlock(block);
				putInCache(block.name(), block);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return block;
	}
	
	DataBlock getDataBlock(long uuidHigh, long uuidLow){
		DataBlock block = (DataBlock)getFromCache(DataBlock.name(uuidHigh, uuidLow));
		if (block == null){
			try {
				block = new DataBlock(uuidHigh, uuidLow);
				store.readBlock(block);
				putInCache(block.name(), block);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return block;
	}
	
	DataBlock newDataBlock(){
		UUID uuid = UUID.randomUUID();
		DataBlock block = new DataBlock(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
		putInCache(block.name(), block);
		
		try {
			store.writeBlock(block);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		//System.out.println("\t\t\t Created data block " + uuidStr);
		return block;
	}
	
	void addDirty(Block block){
		dirtyBlocks.add(block);
	}
	
	private synchronized Block getFromCache(String id){
		return get(id);
	}
	
	private synchronized void putInCache(String id, Block block){
		put(id, block);
	}
	
	@Override
	protected boolean removeEldestEntry(Map.Entry<String, Block> eldest) {
		boolean remove = size() > Constants.maxBlocksInCache;
		if (remove && eldest.getValue().dirty()){
			dirtyBlocks.add(eldest.getValue());
			
			System.out.println("\t\t Evicted block from cache: " + eldest.getValue().name());
		}
		
		return remove;
	}
	
	private void runBlocksWriter(){
		blocksWriter = new Thread(){
			public void run(){
				while(!stop){
					Block block = null;
					try {
						block = dirtyBlocks.take();
					} catch (InterruptedException e) {
						break;
					}
					
					try {
						if (block.dirty()) {
							store.writeBlock(block);
							block.clearDirty();
						}
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		};
		blocksWriter.start();
	}
	
	void stop(){
		System.out.println("Closing cache.");
		stop = true;
		if (blocksWriter != null){
			blocksWriter.interrupt();
			try {
				blocksWriter.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		for (Block block : this.values()){
			if (block.dirty()){
				try {
					store.writeBlock(block);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		this.clear();
		
		Block block = null;
		while((block=dirtyBlocks.poll()) != null){
			if (block.dirty()){
				try {
					store.writeBlock(block);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
}
