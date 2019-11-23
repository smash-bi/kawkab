package kawkab.fs.core.services.thrift;

import kawkab.fs.core.Filesystem;
import kawkab.fs.core.exceptions.KawkabException;
import kawkab.fs.core.exceptions.OutOfMemoryException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.nio.ByteBuffer;
import java.util.List;

public class FilesystemServiceClient {
	private FilesystemService.Client client;
	private TTransport transport;
	private final int MAX_TRIES = 3;

	/**
	 * @param serverIP IP of the server to connect
	 * @param port Filesystem service port
	 * @throws KawkabException
	 */
	public FilesystemServiceClient(String serverIP, int port) throws KawkabException {
		System.out.printf("[FSC] Connecting to %s:%d\n",serverIP,port);
		try {
			transport = new TFramedTransport(new TSocket(serverIP, port));
			transport.open();

			TProtocol protocol = new TBinaryProtocol(transport);
			client = new FilesystemService.Client(protocol);
		} catch (TException x) {
			x.printStackTrace();
			throw new KawkabException(x);
		}
	}

	/**
	 *
	 * @param filename
	 * @param mode FileMode.APPEND creates the file if it does not already exist.
	 * @return Returns a file specific ID that the caller should use in the subsequent file specific functions such as read,
	 *         append, and close.
	 * @throws KawkabException Exceptions from the server are wrapped in KawkabException
	 */
	public long open(String filename, Filesystem.FileMode mode, int recordSize) throws KawkabException {
		try {
			return client.open(filename, convertFileMode(mode), recordSize);
		} catch (TException e) {
			throw new KawkabException(e);
		}
	}

	public ByteBuffer recordNum(long sessionID, long recNum, int recSize) throws KawkabException {
		try {
			return client.recordNum(sessionID, recNum, recSize);
		} catch (TException e) {
			throw new KawkabException(e);
		}
	}

	public ByteBuffer recordAt(long sessionID, long timestamp, int recSize) throws KawkabException {
		try {
			return client.recordAt(sessionID, timestamp, recSize);
		} catch (TException e) {
			throw new KawkabException(e);
		}
	}

	public List<ByteBuffer> readRecords(long sessionID, long minTS, long maxTS, int recSize) throws KawkabException {
		try {
			return client.readRecords(sessionID, minTS, maxTS, recSize);
		} catch (TException e) {
			throw new KawkabException(e);
		}
	}

	/**
	 * @param sessionID
	 * @param buffer
	 * @return Number of bytes appended
	 * @throws KawkabException
	 */
	public int append(long sessionID, ByteBuffer buffer) throws KawkabException {
		try {
			return client.append(sessionID, buffer);
		} catch (TException e) {
			throw new KawkabException(e);
		}
	}

	public int append(long sessionID, ByteBuffer srcBuf, int recSize) throws KawkabException {
		try {
			return client.appendRecord(sessionID, srcBuf, recSize);
		} catch (TException e) {
			throw new KawkabException(e);
		}
	}

	public int appendBuffered(long sessionID, ByteBuffer srcBuf, int recSize) throws OutOfMemoryException, KawkabException {
		int tries = 0;
		while(++tries < MAX_TRIES) {
			try {
				//System.out.printf("pos=%d, lim=%d, recSize=%d\n", srcBuf.position(), srcBuf.limit(), recSize);
				return client.appendRecordBuffered(sessionID, srcBuf, recSize);
			} catch (TOutOfMemoryException e) {
				// Retry if the memory was full
			} catch (TException e) {
				throw new KawkabException(e);
			}
		}

		throw new OutOfMemoryException(String.format("Request failed after %d tries", tries));
	}

	public int appendBatched(long sessionID, List<ByteBuffer> srcBufs, int recSize) throws KawkabException {
		try {
			return client.appendRecordBatched(sessionID, srcBufs, recSize);
		} catch (TException e) {
			throw new KawkabException(e);
		}
	}

	public long size(long sessionID) throws KawkabException {
		try {
			return client.size(sessionID);
		} catch (TException e) {
			throw new KawkabException(e);
		}
	}

	public void close(long sessionID) throws KawkabException {
		try {
			client.close(sessionID);
		} catch (TException e) {
			throw new KawkabException(e);
		}
	}

	public int noop(long none) throws KawkabException {
		try {
			return client.noop(none);
		} catch (TException e) {
			throw new KawkabException(e);
		}
	}

	public synchronized void disconnect() {
		transport.close();

		transport = null;
		client = null;
	}

	private TFileMode convertFileMode(Filesystem.FileMode mode){
		switch(mode) {
			case READ:
				return TFileMode.FM_READ;
			case APPEND:
				return TFileMode.FM_APPEND;
			default:
				return null;
		}
	}

	private void verifyConnected() throws KawkabException {
		if (transport == null || !transport.isOpen())
			throw new KawkabException("Client is not connected");
	}
}
