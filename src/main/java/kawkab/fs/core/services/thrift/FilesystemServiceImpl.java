package kawkab.fs.core.services.thrift;

import kawkab.fs.api.FileOptions;
import kawkab.fs.commons.Configuration;
import kawkab.fs.core.FileHandle;
import kawkab.fs.core.Filesystem;
import kawkab.fs.core.exceptions.KawkabException;
import kawkab.fs.core.services.thrift.FilesystemService.Iface;
import kawkab.fs.core.services.thrift.TRequestFailedException;
import kawkab.fs.core.services.thrift.TInvalidSessionException;
import kawkab.fs.core.services.thrift.TInvalidArgumentException;
import kawkab.fs.core.services.thrift.TFileMode;
import org.apache.thrift.TException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class FilesystemServiceImpl implements Iface {
	private Filesystem fs;
	private Map<Long, FileHandle> sessions;			// Opened files from the clients
	private AtomicLong counter = new AtomicLong();	//To assigns unique sessionsIDs for each file open request from the clients

	private static Configuration conf = Configuration.instance();

	public FilesystemServiceImpl(Filesystem fs) {
		this.fs = fs;
		sessions = new ConcurrentHashMap<>();
	}

	@Override
	public long open(String filename, TFileMode fileMode, int recordSize) throws TException {
		FileHandle handle = null;
		try {
			handle = fs.open(filename, convertFileMode(fileMode), new FileOptions(recordSize));
		} catch (Exception e) {
			throw new TRequestFailedException(e.getMessage());
		}

		long sessionID = counter.incrementAndGet();

		assert handle != null;

		if (sessions.putIfAbsent(sessionID, handle) != null) {
			throw new TRequestFailedException("Session already exist: " + sessionID);
		}

		return sessionID;
	}

	@Override
	public ByteBuffer read(long sessionID, long offset, int length) throws TRequestFailedException, TInvalidSessionException, TInvalidArgumentException, TException {
		FileHandle fh = sessions.get(sessionID);

		if (fh == null) {
			throw new TInvalidSessionException("Session ID is invalid or the session does not exist.");
		}

		if (length > conf.maxBufferLen) {
			throw new TInvalidArgumentException("Maximum allowed length in bytes is "+conf.maxBufferLen);
		}

		long size;
		try {
			size = fh.size();
		} catch (KawkabException e) {
			e.printStackTrace();
			throw new TRequestFailedException(e.getMessage());
		}

		if (offset+length > size) {
			throw new TInvalidArgumentException(String.format("offset+len is greater than file size. offset=%d, length=%d, size=%d",
					offset, length, size));
		}

		byte[] buffer = new byte[length];

		try {
			fh.read(buffer, offset, length);
		} catch (IllegalArgumentException | IOException | KawkabException e) {
			e.printStackTrace();
			throw new TRequestFailedException(e.getMessage());
		}

		return ByteBuffer.wrap(buffer);
	}

	@Override
	public int append(long sessionID, ByteBuffer srcBuf) throws TRequestFailedException, TInvalidSessionException, TException {
		FileHandle fh = sessions.get(sessionID);

		if (fh == null) {
			throw new TInvalidSessionException("Session ID is invalid or the session does not exist.");
		}

		int offset = srcBuf.position();
		int length = srcBuf.remaining();
		try {
			return fh.append(srcBuf.array(), offset, length);
		} catch (Exception e) {
			e.printStackTrace();
			throw new TRequestFailedException(e.getMessage());
		}
	}

	@Override
	public void close(long sessionID) throws TException {
		FileHandle fh = sessions.get(sessionID);
		if (fh == null) {
			System.out.println("[FSS] ERROR: Session ID is invalid or the session does not exist to close " + fh.inumber());
			return;
		}

		try {
			fs.close(fh);
		} catch (KawkabException e) {
			e.printStackTrace();
		}

		sessions.remove(sessionID);
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
}
