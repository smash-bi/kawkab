package kawkab.fs.core;

import kawkab.fs.commons.Configuration;
import org.rocksdb.*;
import org.rocksdb.util.SizeUnit;

import java.io.File;

public final class LocalStoreDBRocksDB implements LocalStoreDBIface {
	private final int id;
	private final int maxSize;
	private final String mapFilePath;
	private static final String mapName = "localStoreDB";
	private byte[] existTestBuffer;
	private RocksDB db; // Path to ID map. A key is a block's path because the data segments belonging
	// to the same block have the same path. In this way, we save the number of
	// entries in the map.
	private int size;

	static {
		RocksDB.loadLibrary();
	}

	public LocalStoreDBRocksDB(int id, int maxSize) {
		this.id = id;
		this.maxSize = maxSize;
		mapFilePath = Configuration.instance().basePath + "/localStoreDB/chroniclemap-"+Configuration.instance().thisNodeID+"-"+id;
		initMap();
	}

	private void initMap() {
		System.out.println("Initializing RocksDB: " + mapFilePath);

		File file = new File(mapFilePath).getParentFile();
		if (!file.exists()) {
			file.mkdirs();
		}

		try (final Options options = new Options();
		) {
			try {
				options.setCreateIfMissing(true)
						.setAllowConcurrentMemtableWrite(true)
						.setWriteBufferSize(4 * SizeUnit.MB)
						.setMaxWriteBufferNumber(3)
						.setCompressionType(CompressionType.LZ4_COMPRESSION)
						.setCompactionStyle(CompactionStyle.UNIVERSAL);
			} catch (final IllegalArgumentException e) {
				assert (false);
			}

			/*options.setMemTableConfig(
					new HashSkipListMemTableConfig()
							.setHeight(3)
							.setBranchingFactor(3)
							.setBucketCount(100000));*/

			try {
				db = RocksDB.open(options, mapFilePath);
			} catch (final RocksDBException e) {
				System.out.format("Caught the expected exception -- %s\n", e);
			}

			existTestBuffer = new byte[500];
			size = getSize();

			//Compact db
			// Get the db size
		}
	}

	@Override
	public synchronized void put(BlockID id) {
		//System.out.println("[LSDB] Added: " + id.name());

		//assert db.size()+1 <= maxSize; //FIXME: Ensure that the number of entries is limited

		try {
			db.put(id.localPath().getBytes(), existTestBuffer);
		} catch (RocksDBException e) {
			e.printStackTrace();
			return;
		}

		size++;

		//System.out.println("Created block: "  + id +", "+ id.localPath());
	}

	/**
	 * @return Whether the blockName entry exists in the DB or not
	 */
	@Override
	public synchronized boolean exists(BlockID id) {
		//System.out.println("[LSDB] Exists: " + id.name());
		byte[] found = null;
		try {
			found = db.get(id.localPath().getBytes());
		} catch (RocksDBException e) {
			e.printStackTrace();
			return false;
		}

		return found != null;
	}

	/**
	 * @return Location of the blockName, or null if blockName entry was not in the DB.
	 */
	@Override
	public synchronized String removeEntry(BlockID id) {
		//System.out.println("[LSDB] Removed: " + id.localPath());
		try {
			db.delete(id.localPath().getBytes());
			size--;
			return id.localPath(); //FIXME: we should return the value we get from DB
		} catch (RocksDBException e) {
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public synchronized int size() {
		return size;
	}

	@Override
	public synchronized void shutdown() {
		db.close();
	}

	private synchronized int getSize() {
		String prop;
		try {
			db.compactRange(); //FIXME: Verify if we are really compacting data and have correct size of the DB
			int numKeys = (int)db.getLongProperty("rocksdb.estimate-num-keys");
			assert numKeys >=0;
			return numKeys;
		} catch (RocksDBException e) {
			e.printStackTrace();
			return -1;
		}
	}
}
