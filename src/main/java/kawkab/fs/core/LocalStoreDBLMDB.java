package kawkab.fs.core;

import kawkab.fs.commons.Configuration;
import org.lmdbjava.*;

import java.io.File;
import java.nio.ByteBuffer;

import static org.lmdbjava.SeekOp.MDB_FIRST;

public final class LocalStoreDBLMDB implements LocalStoreDBIface {
	private final int id;
	private final int maxSize;
	private final String mapFilePath;
	private static final String mapName = "localStoreDB";
	private Dbi<ByteBuffer> db; // Path to ID map. A key is a block's path because the data segments belonging
	                                          // to the same block have the same path. In this way, we save the number of
	                                          // entries in the map.
	private Env<ByteBuffer> dbEnv;

	private final ByteBuffer key = ByteBuffer.allocateDirect(500);
	private final ByteBuffer val = ByteBuffer.allocateDirect(10);
	private int size;

	public LocalStoreDBLMDB(int id, int maxSize) {
		this.id = id;
		this.maxSize = maxSize;
		mapFilePath = Configuration.instance().basePath + "/localStoreDB/chroniclemap-"+Configuration.instance().thisNodeID+"-"+id;
		initMap();
	}
	
	private void initMap() {
		System.out.println("Initializing LMDB: " + mapFilePath);
		File file = new File(mapFilePath).getParentFile();
		if (!file.exists()) {
			file.mkdirs();
		}

		dbEnv = org.lmdbjava.Env.create()
				// LMDB also needs to know how large our DB might be. Over-estimating is OK.
				.setMapSize(1_485_760)
				// LMDB also needs to know how many DBs (Dbi) we want to store in this Env.
				.setMaxDbs(1)
				// Now let's open the Env. The same path can be concurrently opened and
				// used in different processes, but do not open the same path twice in
				// the same process at the same time.
				.open(file);

		db = dbEnv.openDbi(mapName, DbiFlags.MDB_CREATE);
		size = getSize();
	}
	
	public synchronized void put(BlockID id) {
		//System.out.println("[LSDB] Added: " + id.name());
		
		assert size+1 <= maxSize;

		key.clear();
		key.put(id.localPath().getBytes()).flip();
		db.put(key, val);
		size++;
		
		//System.out.println("Created block: "  + id +", "+ id.localPath());
	}
	
	/**
	 * @return Whether the blockName entry exists in the DB or not
	 */
	public synchronized boolean exists(BlockID id) {
		//System.out.println("[LSDB] Exists: " + id.name());

		key.clear();
		key.put(id.localPath().getBytes()).flip();
		try (Txn<ByteBuffer> txn = dbEnv.txnRead()) {
			final ByteBuffer found = db.get(txn, key);
			return found != null;
			//FIXME: Do we need to verify that the value points to the same block?
		}
	}

	/**
	 * @return Location of the blockName, or null if blockName entry was not in the DB.
	 */
	@Override
	public synchronized String removeEntry(BlockID id) {
		//System.out.println("[LSDB] Removed: " + id.localPath());
		key.clear();
		key.put(id.localPath().getBytes()).flip();
		db.delete(key);

		size --;
		assert size >= 0;

		return id.localPath(); //FIXME: The return value should come from the value obtained from the DB
	}

	@Override
	public synchronized int size() {
		return size;
	}

	public synchronized void shutdown() {
		dbEnv.close();
		db.close();
	}

	private synchronized int getSize(){
		try (Txn<ByteBuffer> txn = dbEnv.txnRead()) {
			// A cursor always belongs to a particular Dbi.
			final Cursor<ByteBuffer> c = db.openCursor(txn);
			c.seek(MDB_FIRST);
			int count = 0;
			while (c.next()) {
				count++;
			}
			c.close();

			return count;
		}
	}
}
