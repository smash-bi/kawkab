package kawkab.fs.core.services.thrift;

import kawkab.fs.api.FileOptions;
import kawkab.fs.api.Record;
import kawkab.fs.commons.Configuration;
import kawkab.fs.core.FileHandle;
import kawkab.fs.core.Filesystem;
import kawkab.fs.core.exceptions.KawkabException;
import kawkab.fs.core.exceptions.OutOfMemoryException;
import kawkab.fs.core.services.thrift.FilesystemService.Iface;
import kawkab.fs.records.SampleRecord;
import org.apache.thrift.TException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class FilesystemServiceImpl implements Iface {
	private Filesystem fs;
	private Map<Integer, Session> sessions;			// Opened files from the clients
	private AtomicInteger counter = new AtomicInteger();	//To assigns unique sessionsIDs for each file open request from the clients

	private static Configuration conf = Configuration.instance();

	private static ThreadLocal<ByteBuffer> thrLocalBuf = ThreadLocal.withInitial(() -> ByteBuffer.allocate(conf.maxBufferLen));

	public FilesystemServiceImpl(Filesystem fs) {
		this.fs = fs;
		sessions = new ConcurrentHashMap<>();
	}

	@Override
	public int open(String filename, TFileMode fileMode, int recordSize) throws TException {
		//System.out.printf("[FSI] open(): opening file %s, recSize %d\n", filename, recordSize);

		FileHandle handle = null;
		try {
			handle = fs.open(filename, convertFileMode(fileMode), new FileOptions(recordSize));
		} catch (Exception | AssertionError e) {
			e.printStackTrace();
			throw new TRequestFailedException(e.getMessage());
		}

		int sessionID = counter.incrementAndGet();
		int recSize = -1;
		try {
			recSize = handle.recordSize();
		} catch (OutOfMemoryException e) {
			e.getMessage();
			throw new TOutOfMemoryException(e.getMessage());
		}  catch (Exception | AssertionError e) {
			e.printStackTrace();
			throw new TRequestFailedException(e.getMessage());
		}

		Session session = new Session(sessionID, recSize, handle);
		if (sessions.putIfAbsent(sessionID, session) != null) {
			throw new TRequestFailedException("Session already exist: " + sessionID);
		}

		return sessionID;
	}

	@Override
	public List<Integer> bulkOpen(List<TFileOpenRequest> fopenReqs) throws TRequestFailedException, TException {
		List<Integer> retVals = new ArrayList<>(fopenReqs.size());
		for (TFileOpenRequest req : fopenReqs) {
			retVals.add(open(req.filename, req.fileMode, req.recordSize));
		}

		return retVals;
	}

	@Override
	public ByteBuffer recordNum(int sessionID, long recNum, int recSize, boolean loadFromPrimary) throws TInvalidSessionException, TRequestFailedException, TOutOfMemoryException {
		Session s = sessions.get(sessionID);
		if (s == null) {
			throw new TInvalidSessionException("Session ID is invalid or the session does not exist.");
		}
		FileHandle fh = s.fh;

		ByteBuffer dstBuf = thrLocalBuf.get();
		dstBuf.clear();
		try {
			fh.recordNum(dstBuf, recNum, recSize, loadFromPrimary);
			return dstBuf;
		} catch (OutOfMemoryException e) {
			e.getMessage();
			throw new TOutOfMemoryException(e.getMessage());
		}  catch (Exception | AssertionError e) {
			e.printStackTrace();
			throw new TRequestFailedException(e.getMessage());
		}
	}

	@Override
	public ByteBuffer recordAt(int sessionID, long timestamp, int recSize, boolean loadFromPrimary) throws TRequestFailedException, TInvalidSessionException, TOutOfMemoryException {
		Session s = sessions.get(sessionID);
		if (s == null) {
			throw new TInvalidSessionException("Session ID is invalid or the session does not exist.");
		}
		FileHandle fh = s.fh;

		ByteBuffer dstBuf = thrLocalBuf.get();
		dstBuf.clear();

		try {
			if (!fh.recordAt(dstBuf, timestamp, recSize, loadFromPrimary)) {
				throw new TRequestFailedException("Record not found");
			}

			return dstBuf;
		} catch (OutOfMemoryException e) {
			e.getMessage();
			throw new TOutOfMemoryException(e.getMessage());
		} catch (Exception | AssertionError e) {
			e.printStackTrace();
			throw new TRequestFailedException(e.getMessage());
		}
	}

	@Override
	public List<ByteBuffer> readRecords(int sessionID, long minTS, long maxTS, int recSize, boolean loadFromPrimary) throws TRequestFailedException, TInvalidSessionException, TOutOfMemoryException {
		Session s = sessions.get(sessionID);
		if (s == null) {
			throw new TInvalidSessionException("Session ID is invalid or the session does not exist.");
		}
		FileHandle fh = s.fh;

		try {
			List<ByteBuffer> results = fh.readRecords(minTS, maxTS, recSize, loadFromPrimary);
			//printBuffers(results);
			return results;
		} catch (OutOfMemoryException e) {
			e.getMessage();
			throw new TOutOfMemoryException(e.getMessage());
		} catch (Exception | AssertionError e) {
			e.printStackTrace();
			throw new TRequestFailedException(e.getMessage());
		}
	}

	private void printBuffers(List<ByteBuffer> results) {
		Record recFactory = new SampleRecord();
		int recSize = recFactory.size();

		for (ByteBuffer buf : results) {
			int remaining = buf.remaining();

			int idx = 0;
			while (remaining >= recSize) {
				int offset = idx++*recSize;
				buf.position(offset);
				buf.limit(offset+recSize);

				Record rec = recFactory.newRecord();
				rec.copyInDstBuffer().put(buf);
				System.out.printf("[FSI] Sending record: %s\n",rec);

				remaining -= recSize;
			}

			buf.rewind();
		}
	}

	@Override
	public int appendRecord(int sessionID, ByteBuffer data, int recSize) throws TRequestFailedException, TInvalidSessionException, TOutOfMemoryException {
		Session s = sessions.get(sessionID);
		if (s == null) {
			throw new TInvalidSessionException("Session ID is invalid or the session does not exist.");
		}
		FileHandle fh = s.fh;

		try {
			return fh.append(data, recSize);
		} catch (OutOfMemoryException e) {
			e.getMessage();
			throw new TOutOfMemoryException(e.getMessage());
		} catch (Exception | AssertionError e) {
			e.printStackTrace();
			throw new TRequestFailedException(e.getMessage());
		}
	}

	@Override
	public int appendRecordBuffered(int sessionID, ByteBuffer srcBuf, int recSize) throws TOutOfMemoryException, TRequestFailedException, TInvalidSessionException {
		Session s = sessions.get(sessionID);
		if (s == null) {
			throw new TInvalidSessionException("Session ID is invalid or the session does not exist.");
		}
		FileHandle fh = s.fh;

		try {
			return fh.append(srcBuf, recSize);
		} catch (OutOfMemoryException e) {
			e.getMessage();
			throw new TOutOfMemoryException(e.getMessage());
		} catch (Exception | AssertionError e) {
			e.printStackTrace();
			throw new TRequestFailedException(e.getMessage());
		}

		/*int pos = srcBuf.position();
		int initLimit = srcBuf.limit();
		int cnt = 0;
		while(pos < initLimit) {
			try {
				srcBuf.position(pos);
				srcBuf.limit(pos+recSize);
				cnt += fh.append(srcBuf, recSize);
				pos += recSize;
			} catch (Exception | AssertionError e) {
				e.printStackTrace();
				throw new TRequestFailedException(e.getMessage());
			}
		}
		return cnt;*/
	}

	/**
	 * Appends records batched in the buffer. The buffer contains the pairs {sessionID, record}.
	 * @param data
	 * @return Returns the number of records appended
	 * @throws TRequestFailedException
	 * @throws TInvalidSessionException
	 * @throws TOutOfMemoryException
	 * @throws TException
	 */
	@Override
	public int appendRecords(ByteBuffer data) throws TRequestFailedException, TInvalidSessionException, TOutOfMemoryException, TException {
		int pos = data.position();
		int limit = data.limit();

		assert data.remaining() >= Integer.BYTES;

		int cnt = 0;
		while(pos < limit) {
			int sessionID = data.getInt();

			Session s = sessions.get(sessionID);
			if (s == null) {
				throw new TInvalidSessionException("Session ID is invalid or the session does not exist.");
			}
			FileHandle fh = s.fh;

			pos += Integer.BYTES + s.recSize;
			data.limit(pos);

			try {
				fh.append(data, s.recSize);
			} catch (OutOfMemoryException e) {
				e.getMessage();
				throw new TOutOfMemoryException(e.getMessage());
			} catch (Exception | AssertionError e) {
				e.printStackTrace();
				throw new TRequestFailedException(e.getMessage());
			}

			data.limit(limit);
			cnt++;
		}

		return cnt;
	}

	@Override
	public int appendNoops(ByteBuffer data) throws TRequestFailedException, TInvalidSessionException, TOutOfMemoryException, TException {
		ByteBuffer buffer = thrLocalBuf.get();
		buffer.clear();
		buffer.put(data);
		return 0;
	}

	@Override
	public int appendRecordBatched(int sessionID, List<ByteBuffer> data, int recSize) throws TRequestFailedException, TInvalidSessionException, TOutOfMemoryException {
		Session s = sessions.get(sessionID);
		if (s == null) {
			throw new TInvalidSessionException("Session ID is invalid or the session does not exist.");
		}
		FileHandle fh = s.fh;

		int cnt = 0;
		for(ByteBuffer srcBuf : data) {
			//data.position(offset);
			try {
				cnt += fh.append(srcBuf, recSize);
			} catch (OutOfMemoryException e) {
				e.getMessage();
				throw new TOutOfMemoryException(e.getMessage());
			} catch (Exception | AssertionError e) {
				e.printStackTrace();
				throw new TRequestFailedException(e.getMessage());
			}
		}
		return cnt;
	}

	@Override
	public ByteBuffer read(int sessionID, long offset, int length, boolean loadFromPrimary) throws TRequestFailedException, TInvalidSessionException, TInvalidArgumentException, TOutOfMemoryException {
		Session s = sessions.get(sessionID);
		if (s == null) {
			throw new TInvalidSessionException("Session ID is invalid or the session does not exist.");
		}
		FileHandle fh = s.fh;

		if (length > conf.maxBufferLen) {
			throw new TInvalidArgumentException("Maximum allowed length in bytes is "+conf.maxBufferLen);
		}

		long size;
		try {
			size = fh.size();
		} catch (Exception | AssertionError e) {
			e.printStackTrace();
			throw new TRequestFailedException(e.getMessage());
		}

		if (offset+length > size) {
			throw new TInvalidArgumentException(String.format("offset+len is greater than file size. offset=%d, length=%d, size=%d",
					offset, length, size));
		}

		byte[] buffer = new byte[length];

		try {
			fh.read(buffer, offset, length, loadFromPrimary);
		} catch (OutOfMemoryException e) {
			e.getMessage();
			throw new TOutOfMemoryException(e.getMessage());
		} catch (Exception | AssertionError e) {
			e.printStackTrace();
			throw new TRequestFailedException(e.getMessage());
		}

		return ByteBuffer.wrap(buffer);
	}

	@Override
	public int append(int sessionID, ByteBuffer srcBuf) throws TRequestFailedException, TInvalidSessionException, TOutOfMemoryException {
		Session s = sessions.get(sessionID);
		if (s == null) {
			throw new TInvalidSessionException("Session ID is invalid or the session does not exist.");
		}
		FileHandle fh = s.fh;

		int offset = srcBuf.position();
		int length = srcBuf.remaining();
		try {
			return fh.append(srcBuf.array(), offset, length);
		} catch (OutOfMemoryException e) {
			e.getMessage();
			throw new TOutOfMemoryException(e.getMessage());
		} catch (Exception | AssertionError e) {
			e.printStackTrace();
			throw new TRequestFailedException(e.getMessage());
		}
	}

	@Override
	public long size(int sessionID) throws TRequestFailedException, TInvalidSessionException, TOutOfMemoryException {
		Session s = sessions.get(sessionID);
		if (s == null) {
			throw new TInvalidSessionException("Session ID is invalid or the session does not exist.");
		}
		FileHandle fh = s.fh;

		try {
			return fh.size();
		} catch (OutOfMemoryException e) {
			e.getMessage();
			throw new TOutOfMemoryException(e.getMessage());
		} catch (Exception | AssertionError e) {
			e.printStackTrace();
			throw new TRequestFailedException(e.getMessage());
		}
	}

	@Override
	public int recordSize(int sessionID) throws TRequestFailedException, TInvalidSessionException, TOutOfMemoryException {
		Session s = sessions.get(sessionID);
		if (s == null) {
			throw new TInvalidSessionException("Session ID is invalid or the session does not exist.");
		}
		FileHandle fh = s.fh;

		try {
			return fh.recordSize();
		} catch (OutOfMemoryException e) {
			e.getMessage();
			throw new TOutOfMemoryException(e.getMessage());
		} catch (Exception | AssertionError e) {
			e.printStackTrace();
			throw new TRequestFailedException(e.getMessage());
		}
	}

	@Override
	public void close(int sessionID) throws TException {
		Session s = sessions.get(sessionID);
		if (s == null) {
			throw new TInvalidSessionException("Session ID is invalid or the session does not exist.");
		}
		FileHandle fh = s.fh;

		try {
			fs.close(fh);
		} catch (Exception | AssertionError e) {
			e.printStackTrace();
		}

		sessions.remove(sessionID);
	}

	@Override
	public void bulkClose(List<Integer> ids) throws TException {
		for (int id : ids) {
			close(id);
		}
	}

	@Override
	public int flush() throws TException {
		try {
			fs.flush();
			flushOSCache();
		} catch (KawkabException | InterruptedException | IOException e) {
			e.printStackTrace();
		}

		return 0;
	}

	private void flushOSCache() throws InterruptedException, IOException {
		Runtime run = Runtime.getRuntime(); // get OS Runtime
		// execute a system command and give back the process
		Process pr = run.exec("sudo sync; sudo echo 1 > /proc/sys/vm/drop_caches");
		pr.waitFor();
	}

	@Override
	public int noopWrite(long none) throws TRequestFailedException, TInvalidSessionException {
		return 0;
	}

	@Override
	public ByteBuffer noopRead(int recSize) throws TRequestFailedException, TInvalidSessionException {
		return ByteBuffer.allocate(recSize);
	}

	private Filesystem.FileMode convertFileMode(TFileMode mode){
		switch(mode) {
			case FM_READ:
				return Filesystem.FileMode.READ;
			case FM_APPEND:
				return Filesystem.FileMode.APPEND;
			default:
				return null;
		}
	}

	private class Session {
		private final int id;
		private final int recSize;
		private final FileHandle fh;

		private Session(int id, int recSize, FileHandle fh) {
			this.id = id;
			this.recSize = recSize;
			this.fh = fh;
		}
	}
}
