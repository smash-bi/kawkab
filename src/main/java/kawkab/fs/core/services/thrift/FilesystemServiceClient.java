package kawkab.fs.core.services.thrift;

import kawkab.fs.core.FileHandle;
import kawkab.fs.core.Filesystem;
import kawkab.fs.core.exceptions.KawkabException;
import kawkab.fs.core.services.thrift.TFileMode;
import kawkab.fs.core.services.thrift.FilesystemService;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class FilesystemServiceClient {
	private FilesystemService.Client client;
	private TTransport transport;

	/**
	 * @param serverIP IP of the server to connect
	 * @param port Filesystem service port
	 * @throws KawkabException
	 */
	public FilesystemServiceClient(String serverIP, int port) throws KawkabException {
		System.out.printf("[FSC] Connecting to %s:%d\n",serverIP,port);
		try {
			transport = new TSocket(serverIP, port);
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
	public synchronized long open(String filename, Filesystem.FileMode mode, int recordSize) throws KawkabException{
		try {
			return client.open(filename, convertFileMode(mode), recordSize);
		} catch (TException e) {
			throw new KawkabException(e);
		}
	}

	public synchronized ByteBuffer recordNum(long sessionID, long recNum, int recSize) throws KawkabException {
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
	public synchronized int append(long sessionID, ByteBuffer buffer) throws KawkabException {
		try {
			return client.append(sessionID, buffer);
		} catch (TException e) {
			throw new KawkabException(e);
		}
	}

	public synchronized int append(long sessionID, ByteBuffer srcBuf, long timestamp, int recSize) throws KawkabException {
		try {
			return client.appendRecord(sessionID, srcBuf, timestamp, recSize);
		} catch (TException e) {
			throw new KawkabException(e);
		}
	}

	public synchronized long size(long sessionID) throws KawkabException {
		try {
			return client.size(sessionID);
		} catch (TException e) {
			throw new KawkabException(e);
		}
	}

	public synchronized void close(long sessionID) throws KawkabException {
		try {
			client.close(sessionID);
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
}
