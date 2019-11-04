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

	public KClient (int id) {
		this.id = id;
		sessions = new HashMap<>();
	}

	public void connect(String ip, int port) throws KawkabException {
		if (client != null) {
			System.out.printf("Client %d is already connected to %s:%d, not connecting again\n", id, ip, port);
			return;
		}

		client = new FilesystemServiceClient(ip, port);

		this.ip = ip;
		this.port = port;

		System.out.printf("[KC] Connected to %s:%d\n", ip, port);
	}

	public long open(String fn, Filesystem.FileMode mode, Record recFactory) throws KawkabException {
		assert client != null;

		long id = client.open(fn, mode, recFactory.size());

		Session session = new Session(id, recFactory, mode);
		sessions.put(fn, session);

		return id;
	}

	public long open(String fn, Filesystem.FileMode mode) throws KawkabException {
		assert client != null;

		long id = client.open(fn, mode, 1);

		Session session = new Session(id, null, mode);
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

		return client.append(session.id, rec.copyOutSrcBuffer(), rec.timestamp(), rec.size());
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

		List<Record> recs = new ArrayList<>(results.size());

		for (ByteBuffer buf : results) {
			int recSize = recFactory.size();
			int remaining = buf.remaining();

			int idx = 0;
			while (remaining >= recSize) {
				int offset = idx++*recSize;
				buf.position(offset);
				buf.limit(offset+recSize);

				Record rec = recFactory.newRecord();
				rec.copyInDstBuffer().put(buf);
				recs.add(rec);

				remaining -= recSize;
			}
		}

		return recs;
	}

	public void close(String fn) throws KawkabException {
		assert client != null;
		Session session = sessions.get(fn);
		if (session == null)
			throw new KawkabException(String.format("File %s is not opened",fn));

		client.close(session.id);
	}

	public void disconnect() throws KawkabException {
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

	private class Session {
		private final long id;
		private final Record recFactory;
		private final Filesystem.FileMode mode;

		private Session(long id, Record recFactory, Filesystem.FileMode mode) {
			this.id = id;
			this.recFactory = recFactory;
			this.mode = mode;
		}
	}

	public String ip(){
		return ip;
	}

	public int port() {
		return port;
	}
}
