package kawkab.fs.core;

import com.google.common.cache.*;
import kawkab.fs.commons.Configuration;
import kawkab.fs.core.exceptions.KawkabException;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class GCache extends Cache implements RemovalListener<BlockID, Block>{
	private static final Object initLock = new Object();
	private static GCache instance;
	
	private LoadingCache<BlockID, Block> cache;
	private LocalStoreManager localStore;
	private static Configuration conf;
	private final int MAX_BLOCKS_IN_CACHE;
	
	private GCache() {
		System.out.println("Initializing cache..." );
		
		conf = Configuration.instance();
		
		localStore = LocalStoreManager.instance();
		//cache = new GCache(this);
		
		MAX_BLOCKS_IN_CACHE = (int)(conf.cacheSizeMiB / (conf.segmentSizeBytes/1048576.0) + conf.inodeBlocksPerMachine + conf.ibmapsPerMachine); //FIXME: Not calculated correctly
		assert MAX_BLOCKS_IN_CACHE > 0;
		
		cache = CacheBuilder.newBuilder()
				.maximumSize(MAX_BLOCKS_IN_CACHE)
				.initialCapacity(MAX_BLOCKS_IN_CACHE)
				//.expireAfterAccess(120, TimeUnit.SECONDS)
				.removalListener(this)
				.concurrencyLevel(8)
				.build(new CacheLoader<BlockID, Block>() {
			@Override
			public Block load(BlockID key) throws Exception {
				return  key.newBlock();
			}
		});
	}
	
	public static GCache instance() {
		if (instance == null) {
			synchronized(initLock) {
				if (instance == null) {
					instance = new GCache();
				}
			}
		}
		
		return instance;
	}
	
	@Override
	public Block acquireBlock(final BlockID blockID) throws IOException, KawkabException {
		Block block = null;
		try {
			block = cache.get(blockID);
		} catch (ExecutionException e) {
			e.printStackTrace();
			return null;
		}
		
		assert block != null;
		
		block.loadBlock(); // Loads data into the block based on the block's load policy. The loadBlock
									// function
									// deals with concurrency. Therefore, we don't need to provide mutual exclusion
									// here.
		return block;
			
	}
	
	@Override
	public void releaseBlock(BlockID blockID) throws KawkabException {
		Block block = null;
		
		try {
			block = cache.get(blockID);
		} catch (ExecutionException e) {
			e.printStackTrace();
			return;
		}
		
		if (blockID.onPrimaryNode() && block.isLocalDirty()) { // Persist blocks only through the primary node
			// If the dirty bit for the local store is set
			localStore.store(block); // The call is non-blocking. Multiple threads are allowed 
									 // to add the same block in the queue.
			// FIXME: What to do with the exception?
		}
	}
	
	@Override
	public void flush() throws KawkabException {
		Map<BlockID, Block> map = cache.asMap();
		for(Block block : map.values()) {
			if (block.isLocalDirty()) {
				try {
					block.waitUntilSynced();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		
		cache.invalidateAll();
	}
	
	@Override
	public void shutdown() throws KawkabException {
		System.out.println("Closing cache. Current size = "+cache.size());
		flush();
		localStore.shutdown();
	}

	@Override
	public void onRemoval(RemovalNotification<BlockID, Block> notification) {
		Block block = notification.getValue();
		
		assert block != null;
		
		//System.out.println("Evicting from cache: "+block.id());
		
		//try {
			//block.waitUntilSynced();  // FIXME: This is a blocking call and the cacheLock is locked. This may
	                                  // lead to performance problems because the thread sleeps while holding
	                                  // the cacheLock. The lock cannot be released because otherwise another
	                                  // thread can come and may acquire the block.
			//localStore.notifyEvictedFromCache(block);
		//} catch (KawkabException e) {
		//	e.printStackTrace();
		//}
	}
	
	//FIXME: Currently a cache entry can be evicted from the cache even if the block is dirty.
	
	@Override
	public long size() {
		return cache.size();
	}

	@Override
	public String getStats() {
		return null;
	}

	@Override
	public void printStats() {
	}

	@Override
	public void resetStats(){}
}
