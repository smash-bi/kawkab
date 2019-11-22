package kawkab.fs.client;

import kawkab.fs.api.Record;
import kawkab.fs.core.Filesystem;
import kawkab.fs.core.exceptions.KawkabException;
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

	private static int BUFLEN_BYTES = 16 * 1024; // Maximum data size per RPC

	public KClient (int id) {
		this.id = id;
		sessions = new HashMap<>();
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

	public long open(String fn, Filesystem.FileMode mode, int recSize) throws KawkabException {
		assert client != null;

		if (sessions.containsKey(fn)){
			throw new KawkabException(String.format("File %s is already opened",fn));
		}

		long id = client.open(fn, mode, recSize);

		Session session = new Session(id, mode);
		sessions.put(fn, session);

		return id;
	}

	public long open(String fn, Filesystem.FileMode mode) throws KawkabException {
		assert client != null;

		if (sessions.containsKey(fn)){
			throw new KawkabException(String.format("File %s is already opened",fn));
		}

		long id = client.open(fn, mode, 1);

		Session session = new Session(id, mode);
		sessions.put(fn, session);

		return id;
	}

	public long size(String fn) throws KawkabException {
		assert client != null;

		Session session = sessions.get(fn);
		if (session == null)
			throw new KawkabException(String.format("File %s is not opened",fn));

		return client.size(session.id);
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

	public int appendBuffered(String fn, Record[] records, int recSize) throws KawkabException {
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

	public Record recordNum(String fn, long recNum, Record recFactory) throws KawkabException {
		assert client != null;
		Session session = sessions.get(fn);
		if (session == null)
			throw new KawkabException(String.format("File %s is not opened",fn));

		ByteBuffer buffer = client.recordNum(session.id, recNum, recFactory.size());

		Record rec = recFactory.newRecord();
		rec.copyInDstBuffer().put(buffer);
		return rec;
	}

	public Record recordAt(String fn, long timestamp, Record recFactory) throws KawkabException {
		assert client != null;
		Session session = sessions.get(fn);
		if (session == null)
			throw new KawkabException(String.format("File %s is not opened",fn));

		ByteBuffer buffer = client.recordAt(session.id, timestamp, recFactory.size());

		Record rec = recFactory.newRecord();
		rec.copyInDstBuffer().put(buffer);
		return rec;
	}

	public List<Record> readRecords(String fn, long minTS, long maxTS, Record recFactory) throws KawkabException {
		assert client != null;
		Session session = sessions.get(fn);
		if (session == null)
			throw new KawkabException(String.format("File %s is not opened",fn));

		List<ByteBuffer> results = client.readRecords(session.id, minTS, maxTS, recFactory.size());

		//printBuffers(results, recFactory);

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

	public int noop(int none) throws KawkabException {
		assert client != null;
		return client.noop(none);
	}

	public void close(String fn) throws KawkabException {
		assert client != null;
		Session session = sessions.get(fn);
		if (session == null)
			throw new KawkabException(String.format("File %s is not opened",fn));

		client.close(session.id);
		sessions.remove(fn);
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
		private final long id;
		private final Filesystem.FileMode mode;

		private Session(long id, Filesystem.FileMode mode) throws KawkabException {
			this.id = id;
			this.mode = mode;
		}
	}
}
