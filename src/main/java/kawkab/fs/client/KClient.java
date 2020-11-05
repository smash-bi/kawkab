package kawkab.fs.client;

import kawkab.fs.api.Record;
import kawkab.fs.core.Filesystem;
import kawkab.fs.core.exceptions.KawkabException;
import kawkab.fs.core.exceptions.OutOfMemoryException;
import kawkab.fs.core.services.thrift.FilesystemServiceClient;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KClient {
	private final int id;
	private FilesystemServiceClient client;
	private String ip;
	private int port;
	private Map<String, Session> sessions;
	private ByteBuffer buffer;

	private final int DEFAULT_BUFLEN = 2097152;  ///Match with config.properties maxBufferLen parameter
	private final int BUFLEN_BYTES; // = 16 * 1024 * 1024; // Maximum data size per RPC

	public KClient (int id) {
		this.id = id;
		sessions = new HashMap<>();

		BUFLEN_BYTES = Integer.parseInt(System.getProperty("rpcbufferlen", ""+DEFAULT_BUFLEN));

		buffer = ByteBuffer.allocate(BUFLEN_BYTES);

	}

	public synchronized void connect(String ip, int port) throws KawkabException {
		if (client != null) {
			System.out.printf("Client %d is already connected to %s:%d, not connecting again\n", id, ip, port);
			return;
		}

		client = new FilesystemServiceClient(ip, port);

		this.ip = ip;
		this.port = port;

		System.out.printf("[KC] Connected to %s:%d\n", ip, port);
	}

	public int open(String fn, Filesystem.FileMode mode, int recSize) throws KawkabException {
		assert client != null;

		if (sessions.containsKey(fn)){
			throw new KawkabException(String.format("File %s is already opened",fn));
		}

		int id = client.open(fn, mode, recSize);

		Session session = new Session(id, mode);
		sessions.put(fn, session);

		return id;
	}

	public int open(String fn, Filesystem.FileMode mode) throws KawkabException {
		assert client != null;

		if (sessions.containsKey(fn)){
			throw new KawkabException(String.format("File %s is already opened",fn));
		}

		int id = client.open(fn, mode, 1);

		Session session = new Session(id, mode);
		sessions.put(fn, session);

		return id;
	}

	public int bulkOpen(String[] fnames, Filesystem.FileMode[] modes, int[] recSizes) throws KawkabException {
		assert client != null;
		assert fnames.length == modes.length;
		assert modes.length == recSizes.length;

		for (int i=0; i<fnames.length; i++) {
			if (sessions.containsKey(fnames[i])){
				throw new KawkabException(String.format("File %s is already opened",fnames[i]));
			}
		}

		List<Integer> ids = client.bulkOpen(fnames, modes, recSizes);

		assert ids.size() == fnames.length;

		for (int i=0; i<fnames.length; i++) {
			sessions.put(fnames[i], new Session(ids.get(i), modes[i]));
		}

		return ids.size();
	}

	public long size(String fn) throws KawkabException {
		assert client != null;

		Session session = sessions.get(fn);
		if (session == null)
			throw new KawkabException(String.format("File %s is not opened",fn));

		return client.size(session.id);
	}

	public int recordSize(String fn) throws KawkabException {
		assert client != null;

		Session session = sessions.get(fn);
		if (session == null)
			throw new KawkabException(String.format("File %s is not opened",fn));

		return client.recordSize(session.id);
	}

	public int append(String fn, Record rec) throws KawkabException {
		assert client != null;
		Session session = sessions.get(fn);
		if (session == null)
			throw new KawkabException(String.format("File %s is not opened",fn));

		if (session.mode != Filesystem.FileMode.APPEND)
			throw new KawkabException(String.format("File %s is not opened in the append mode.", fn));

		return client.append(session.id, rec.copyOutSrcBuffer(), rec.size());
	}

	public int append(String fn, ByteBuffer srcBuf) throws KawkabException {
		assert client != null;
		Session session = sessions.get(fn);
		if (session == null)
			throw new KawkabException(String.format("File %s is not opened",fn));

		if (session.mode != Filesystem.FileMode.APPEND)
			throw new KawkabException(String.format("File %s is not opened in the append mode.", fn));

		return client.append(session.id, srcBuf);
	}

	public int appendBatched(String fn, Record[] records, int recSize) throws KawkabException {
		assert client != null;
		Session session = sessions.get(fn);
		if (session == null)
			throw new KawkabException(String.format("File %s is not opened",fn));

		if (session.mode != Filesystem.FileMode.APPEND)
			throw new KawkabException(String.format("File %s is not opened in the append mode.", fn));

		List<ByteBuffer> srcBufs = new ArrayList<>(records.length);
		for(Record rec : records) {
			srcBufs.add(rec.copyOutSrcBuffer());
		}

		return client.appendBatched(session.id, srcBufs, recSize);
	}

	public int appendBuffered(String fn, Record[] records, int recSize) throws OutOfMemoryException, KawkabException {
		assert client != null;
		Session session = sessions.get(fn);
		if (session == null)
			throw new KawkabException(String.format("File %s is not opened",fn));

		if (session.mode != Filesystem.FileMode.APPEND)
			throw new KawkabException(String.format("File %s is not opened in the append mode.", fn));

		if (records.length*recSize > BUFLEN_BYTES) {
			throw new KawkabException(String.format("Source data cannot fit in buffer, numRecs=%d, recSize=%d, bufLen=%d",
					records.length, recSize, BUFLEN_BYTES));
		}

		buffer.clear();

		for (Record rec : records) {
			buffer.put(rec.copyOutSrcBuffer());
		}

		buffer.flip();
		return client.appendBuffered(session.id, buffer, recSize);
	}

	public int appendRecordsUnpacked(String[] fnames, Record[] records) throws OutOfMemoryException, KawkabException {
		assert client != null;
		assert fnames.length == records.length;

		buffer.clear();
		for(int i=0; i<fnames.length; i++) {
			String fn = fnames[i];
			Record rec = records[i];
			Session session = sessions.get(fn);
			if (session == null)
				throw new KawkabException(String.format("File %s is not opened",fn));

			buffer.putInt(session.id);
			buffer.put(rec.copyOutSrcBuffer());
		}

		buffer.flip();
		return client.appendRecords(buffer, false);
	}

	private int appendRecordsPacked(Map<Integer, List<Record>> recListsMap) throws OutOfMemoryException, KawkabException {
		buffer.clear();

		//iterate the map and put records per file
		for (Map.Entry<Integer, List<Record>> entry : recListsMap.entrySet()) {
			int sid = entry.getKey();
			List<Record> recs = entry.getValue();

			buffer.putInt(sid);
			buffer.putInt(recs.size());
			for (Record rec : recs) {
				buffer.put(rec.copyOutSrcBuffer());
			}
		}

		buffer.flip();
		return client.appendRecords(buffer, true);
	}

	public int appendRecords(String[] fnames, Record[] records) throws OutOfMemoryException, KawkabException {
		assert client != null;
		assert fnames.length == records.length;

		Map<Integer, List<Record>> recList = new HashMap<>();

		int recsBytes = 0; //Total bytes of records

		//Record list per file
		for(int i=0; i<fnames.length; i++) {
			String fn = fnames[i];
			Session session = sessions.get(fn);
			if (session == null)
				throw new KawkabException(String.format("File %s is not opened",fn));
			int sid = session.id;

			List<Record> recs = recList.get(sid);
			if (recs == null) {
				recs = new ArrayList<>();
				recList.put(sid, recs);
			}
			recs.add(records[i]);

			recsBytes += records[i].size();
		}

		// Count bytes if we use packedBatch
		int packedBytes = 0;
		for (List<Record> recs: recList.values()) {
			packedBytes += 4 + 4 + (recs.size()*recs.get(0).size());
		}

		int unpackedBytes = fnames.length*4 + recsBytes;

		if (packedBytes <= unpackedBytes) {
			return appendRecordsPacked(recList);
		}

		return appendRecordsUnpacked(fnames, records);
	}

	public int appendNoops(String[] fnames, Record[] records) throws OutOfMemoryException, KawkabException {
		assert client != null;
		assert fnames.length == records.length;

		buffer.clear();
		for(int i=0; i<fnames.length; i++) {
			String fn = fnames[i];
			Record rec = records[i];
			Session session = sessions.get(fn);
			if (session == null)
				throw new KawkabException(String.format("File %s is not opened",fn));

			buffer.putInt(session.id);
			buffer.put(rec.copyOutSrcBuffer());
		}

		buffer.flip();
		return client.appendNoops(buffer);
	}

	public Record recordNum(String fn, long recNum, Record recFactory, boolean loadFromPrimary) throws KawkabException {
		assert client != null;
		Session session = sessions.get(fn);
		if (session == null)
			throw new KawkabException(String.format("File %s is not opened",fn));

		ByteBuffer buffer = client.recordNum(session.id, recNum, recFactory.size(), loadFromPrimary);

		Record rec = recFactory.newRecord();
		rec.copyInDstBuffer().put(buffer);
		return rec;
	}

	public Record recordAt(String fn, long timestamp, Record recFactory, boolean loadFromPrimary) throws KawkabException {
		assert client != null;
		Session session = sessions.get(fn);
		if (session == null)
			throw new KawkabException(String.format("File %s is not opened",fn));

		ByteBuffer buffer = client.recordAt(session.id, timestamp, recFactory.size(), loadFromPrimary);

		Record rec = recFactory.newRecord();
		rec.copyInDstBuffer().put(buffer);
		return rec;
	}

	public List<Record> readRecords(String fn, long minTS, long maxTS, Record recFactory, boolean loadFromPrimary) throws KawkabException {
		assert client != null;
		Session session = sessions.get(fn);
		if (session == null)
			throw new KawkabException(String.format("File %s is not opened",fn));

		List<ByteBuffer> results = client.readRecords(session.id, minTS, maxTS, recFactory.size(), loadFromPrimary);

		//printBuffers(results, recFactory);

		if (results.size() == 0)
			return null;

		List<Record> recs = new ArrayList<>(results.size());

		int recSize = recFactory.size();
		for (ByteBuffer buf : results) {
			int initPos = buf.position();
			int initLimit = buf.limit();

			int offset = initPos;
			while (offset+recSize <= initLimit) {
				buf.position(offset);
				buf.limit(offset+recSize);
				offset += recSize;

				Record rec = recFactory.newRecord();
				rec.copyInDstBuffer().put(buf);
				recs.add(rec);
			}
		}

		return recs;
	}

	public int readRecordsCounts(String fn, long minTS, long maxTS, Record recFactory, boolean loadFromPrimary) throws KawkabException {
		assert client != null;
		Session session = sessions.get(fn);
		if (session == null)
			throw new KawkabException(String.format("File %s is not opened",fn));

		List<ByteBuffer> results = client.readRecords(session.id, minTS, maxTS, recFactory.size(), loadFromPrimary);

		//printBuffers(results, recFactory);

		if (results.size() == 0)
			return 0;

		int count = 0;
		int recSize = recFactory.size();
		for (ByteBuffer buf : results) {
			int initPos = buf.position();
			int initLimit = buf.limit();

			int offset = initPos;
			while (offset+recSize <= initLimit) {
				buf.position(offset);
				buf.limit(offset+recSize);
				offset += recSize;
				count++;
			}
		}

		return count;
	}

	/*public void readRecordsAsync(String fn, long minTS, long maxTS, Record recFactory, boolean loadFromPrimary,
								 AsyncMethodCallback<List<ByteBuffer>> resultHandler) throws KawkabException {
		assert client != null;
		Session session = sessions.get(fn);
		if (session == null)
			throw new KawkabException(String.format("File %s is not opened",fn));

		client.readRecordsAsync(session.id, minTS, maxTS, recFactory.size(), loadFromPrimary, resultHandler);
	}*/

	private void printBuffers(List<ByteBuffer> results, Record recFactory) {
		int recSize = recFactory.size();

		for (ByteBuffer buf : results) {
			int remaining = buf.remaining();

			System.out.printf("Before: rem=%d, pos=%d, limit=%d\n", buf.remaining(), buf.position(), buf.limit());

			int idx = 0;
			while (remaining >= recSize) {
				int offset = idx++*recSize;
				buf.position(offset);
				buf.limit(offset+recSize);

				Record rec = recFactory.newRecord();
				rec.copyInDstBuffer().put(buf);
				System.out.printf("[KC] Received record: %s\n",rec);

				remaining -= recSize;
			}

			buf.rewind();
			System.out.printf("After: rem=%d, pos=%d, limit=%d\n", buf.remaining(), buf.position(), buf.limit());
		}
	}

	public int noopWrite(int none) throws KawkabException {
		assert client != null;
		return client.noopWrite(none);
	}

	public ByteBuffer noopRead(int recSize) throws KawkabException {
		assert client != null;
		return client.noopRead(recSize);
	}

	public void flush() {
		assert client != null;
		client.flush();
	}

	public void close(String fn) throws KawkabException {
		assert client != null;
		Session session = sessions.get(fn);
		if (session == null)
			throw new KawkabException(String.format("File %s is not opened",fn));

		client.close(session.id);
		sessions.remove(fn);
	}

	public void bulkClose(String[] fnames) throws KawkabException {
		assert client != null;

		int[] ids = new int[fnames.length];
		for (int i=0; i<fnames.length; i++) {
			Session session = sessions.get(fnames[i]);
			if (session == null)
				throw new KawkabException(String.format("File %s is not opened",fnames[i]));

			ids[i] = session.id;
		}

		client.bulkClose(ids);

		for (int i=0; i<fnames.length; i++) {
			sessions.remove(fnames[i]);
		}
	}

	public synchronized void disconnect() throws KawkabException {
		if (client == null) {
			throw new KawkabException(String.format("Client is not connected to %s:%d.",ip,port));
		}

		for (Session session : sessions.values()) {
			client.close(session.id);
		}

		client.disconnect();
		client = null;
		ip = null;
		port = 0;
	}

	public synchronized void printStats(String fname) throws KawkabException {
		if (client == null) {
			throw new KawkabException(String.format("Client is not connected to %s:%d.",ip,port));
		}

		Session session = sessions.get(fname);
		if (session == null)
			throw new KawkabException(String.format("File %s is not opened",fname));

		client.printStats(session.id);
	}

	public synchronized boolean isConnected() {
		return client != null;
	}

	public String ip(){
		return ip;
	}

	public int port() {
		return port;
	}

	private class Session {
		private final int id;
		private final Filesystem.FileMode mode;

		private Session(int id, Filesystem.FileMode mode) {
			this.id = id;
			this.mode = mode;
		}
	}
}
