package kawkab.fs.core.services.thrift;

import kawkab.fs.core.Filesystem;
import kawkab.fs.core.exceptions.KawkabException;
import kawkab.fs.core.exceptions.OutOfMemoryException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFastFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
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
			transport = new TFastFramedTransport(new TSocket(serverIP, port));
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
	public int open(String filename, Filesystem.FileMode mode, int recordSize) throws KawkabException {
		try {
			return client.open(filename, convertFileMode(mode), recordSize);
		} catch (TException e) {
			throw new KawkabException(e);
		}
	}

	public List<Integer> bulkOpen(String[] fnames, Filesystem.FileMode[] modes, int[] recordSizes) throws KawkabException {
		assert fnames.length == modes.length;
		assert modes.length == recordSizes.length;

		List<TFileOpenRequest> reqs = new ArrayList<>(fnames.length);

		for (int i=0; i<fnames.length; i++) {
			reqs.add(new TFileOpenRequest(fnames[i], convertFileMode(modes[i]), recordSizes[i]));
		}

		try {
			return client.bulkOpen(reqs);
		} catch (TException e) {
			throw new KawkabException(e);
		}
	}

	public ByteBuffer recordNum(int sessionID, long recNum, int recSize, boolean loadFromPrimary) throws KawkabException {
		try {
			return client.recordNum(sessionID, recNum, recSize, loadFromPrimary);
		} catch (TException e) {
			throw new KawkabException(e);
		}
	}

	public ByteBuffer recordAt(int sessionID, long timestamp, int recSize, boolean loadFromPrimary) throws KawkabException {
		try {
			return client.recordAt(sessionID, timestamp, recSize, loadFromPrimary);
		} catch (TException e) {
			throw new KawkabException(e);
		}
	}

	public List<ByteBuffer> readRecords(int sessionID, long minTS, long maxTS, int recSize, boolean loadFromPrimary) throws KawkabException {
		try {
			return client.readRecords(sessionID, minTS, maxTS, recSize, loadFromPrimary);
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
	public int append(int sessionID, ByteBuffer buffer) throws KawkabException {
		try {
			return client.append(sessionID, buffer);
		} catch (TException e) {
			throw new KawkabException(e);
		}
	}

	public int append(int sessionID, ByteBuffer srcBuf, int recSize) throws KawkabException {
		try {
			return client.appendRecord(sessionID, srcBuf, recSize);
		} catch (TException e) {
			throw new KawkabException(e);
		}
	}

	public int appendRecords(ByteBuffer buffer) throws OutOfMemoryException, KawkabException {
		int tries = 0;
		while(++tries < MAX_TRIES) {
			try {
				return client.appendRecords(buffer);
			} catch (TOutOfMemoryException e) {
				// Retry if the memory was full
			} catch (TException e) {
				throw new KawkabException(e);
			}
		}

		throw new OutOfMemoryException(String.format("Request failed after %d tries", tries));
	}

	public int appendNoops(ByteBuffer buffer) throws OutOfMemoryException, KawkabException {
		int tries = 0;
		while(++tries < MAX_TRIES) {
			try {
				return client.appendNoops(buffer);
			} catch (TOutOfMemoryException e) {
				// Retry if the memory was full
			} catch (TException e) {
				throw new KawkabException(e);
			}
		}

		throw new OutOfMemoryException(String.format("Request failed after %d tries", tries));
	}

	public int appendBuffered(int sessionID, ByteBuffer srcBuf, int recSize) throws OutOfMemoryException, KawkabException {
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

	public int appendBatched(int sessionID, List<ByteBuffer> srcBufs, int recSize) throws KawkabException {
		try {
			return client.appendRecordBatched(sessionID, srcBufs, recSize);
		} catch (TException e) {
			throw new KawkabException(e);
		}
	}

	public long size(int sessionID) throws KawkabException {
		try {
			return client.size(sessionID);
		} catch (TException e) {
			throw new KawkabException(e);
		}
	}

	public int recordSize(int sessionID) throws KawkabException {
		try {
			return client.recordSize(sessionID);
		} catch (TException e) {
			throw new KawkabException(e);
		}
	}

	public void close(int sessionID) throws KawkabException {
		try {
			client.close(sessionID);
		} catch (TException e) {
			throw new KawkabException(e);
		}
	}

	public void bulkClose(int[] ids) throws KawkabException {
		List<Integer> idsList = new ArrayList<>(ids.length);
		for (int i=0; i<ids.length; i++) {
			idsList.add(ids[i]);
		}

		try {
			client.bulkClose(idsList);
		} catch (TException e) {
			throw new KawkabException(e);
		}
	}

	public int noopWrite(long none) throws KawkabException {
		try {
			return client.noopWrite(none);
		} catch (TException e) {
			throw new KawkabException(e);
		}
	}

	public ByteBuffer noopRead(int recSize) throws KawkabException {
		try {
			return client.noopRead(recSize);
		} catch (TException e) {
			throw new KawkabException(e);
		}
	}

	public void flush() {
		try {
			client.flush();
		} catch (TException e) {
			e.printStackTrace();
		}
	}

	public void disconnect() {
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
