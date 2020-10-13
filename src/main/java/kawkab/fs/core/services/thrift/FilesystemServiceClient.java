package kawkab.fs.core.services.thrift;

import kawkab.fs.core.Filesystem;
import kawkab.fs.core.exceptions.KawkabException;
import kawkab.fs.core.exceptions.OutOfMemoryException;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.locks.LockSupport;

public class FilesystemServiceClient {
	private FilesystemService.Client sClient; // Synchronous client
	private FilesystemService.AsyncClient aClient; // Asynchronous client
	private TTransport sTransport;
	//private TNonblockingTransport aTransport;
	private final int MAX_TRIES = 30;
	private Random rand = new Random();
	private final int DEFAULT_BUFLEN = 2097152;  ///Match with config.properties maxBufferLen parameter

	/**
	 * @param serverIP IP of the server to connect
	 * @param port Filesystem service port
	 * @throws KawkabException
	 */
	public FilesystemServiceClient(String serverIP, int port) throws KawkabException {
		System.out.printf("[FSC] Connecting to %s:%d\n",serverIP,port);
		int buflen = DEFAULT_BUFLEN;
		try {
			buflen = Integer.parseInt(System.getProperty("rpcbufferlen", ""+DEFAULT_BUFLEN));
		} catch (NumberFormatException e) {
			buflen = DEFAULT_BUFLEN;
		}

		System.out.println("RPC message buffer size bytes: " + buflen);

		try {
			sTransport = new TFastFramedTransport(new TSocket(serverIP, port), buflen, buflen);
			sTransport.open();

			TNonblockingTransport aTransport = new TNonblockingSocket(serverIP, port);
			//aTransport.open();

			TProtocol protocol = new TBinaryProtocol(sTransport);
			sClient = new FilesystemService.Client(protocol);
			aClient = new FilesystemService.AsyncClient(new TBinaryProtocol.Factory(), new TAsyncClientManager(), aTransport);
		} catch (TException | IOException e) {
			e.printStackTrace();
			throw new KawkabException(e);
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
			return sClient.open(filename, convertFileMode(mode), recordSize);
		} catch (TException e) {
			e.printStackTrace();
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
			return sClient.bulkOpen(reqs);
		} catch (TTransportException e) {
			System.out.println("==> Thrift transport exception: " + e.getType());
			throw new KawkabException(e);
		} catch (TException e) {
			throw new KawkabException(e);
		}
	}

	public ByteBuffer recordNum(int sessionID, long recNum, int recSize, boolean loadFromPrimary) throws KawkabException {
		try {
			return sClient.recordNum(sessionID, recNum, recSize, loadFromPrimary);
		} catch (TTransportException e) {
			System.out.println("==> Thrift transport exception: " + e.getType());
			throw new KawkabException(e);
		} catch (TException e) {
			throw new KawkabException(e);
		}
	}

	public ByteBuffer recordAt(int sessionID, long timestamp, int recSize, boolean loadFromPrimary) throws KawkabException {
		try {
			return sClient.recordAt(sessionID, timestamp, recSize, loadFromPrimary);
		} catch (TTransportException e) {
			System.out.println("==> Thrift transport exception: " + e.getType());
			throw new KawkabException(e);
		} catch (TException e) {
			throw new KawkabException(e);
		}
	}

	public List<ByteBuffer> readRecords(int sessionID, long minTS, long maxTS, int recSize, boolean loadFromPrimary) throws KawkabException {
		try {
			return sClient.readRecords(sessionID, minTS, maxTS, recSize, loadFromPrimary);
		} catch (TTransportException e) {
			System.out.println("==> Thrift transport exception: " + e.getType());
			throw new KawkabException(e);
		} catch (TException e) {
			throw new KawkabException(e);
		}
	}

	public void readRecordsAsync(int sessionID, long minTS, long maxTS, int recSize, boolean loadFromPrimary,
											 AsyncMethodCallback<List<ByteBuffer>> resultHandler) throws KawkabException {
		try {
			aClient.readRecords(sessionID, minTS, maxTS, recSize, loadFromPrimary, resultHandler);
		} catch (TTransportException e) {
			System.out.println("==> Thrift transport exception: " + e.getType());
			throw new KawkabException(e);
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
			return sClient.append(sessionID, buffer);
		} catch (TTransportException e) {
			System.out.println("==> Thrift transport exception: " + e.getType());
			throw new KawkabException(e);
		} catch (TException e) {
			throw new KawkabException(e);
		}
	}

	public int append(int sessionID, ByteBuffer srcBuf, int recSize) throws KawkabException {
		try {
			return sClient.appendRecord(sessionID, srcBuf, recSize);
		} catch (TTransportException e) {
			System.out.println("==> Thrift transport exception: " + e.getType());
			throw new KawkabException(e);
		} catch (TException e) {
			throw new KawkabException(e);
		}
	}

	public int appendRecords(ByteBuffer buffer, boolean isPacked) throws OutOfMemoryException, KawkabException {
		int tries = 0;
		int base = 1000;
		int maxTries = 50;
		while(++tries < maxTries) {
			try {
				if (isPacked) {
					return sClient.appendRecordsPacked(buffer);
				}

				return sClient.appendRecords(buffer);
			} catch (TOutOfMemoryException e) {
				System.out.print(".");
				// Retry if the memory was full
				//System.out.println("W " + waitUS);
				int waitUS = base * (1 << tries) + rand.nextInt(100);
				//int waitUS = base * tries + rand.nextInt(100);
				if (waitUS > 4000000)
					waitUS = 4000000;

				if (waitUS > 2000) {
					try {
						Thread.sleep(waitUS / 1000);
					} catch (InterruptedException ex) {
						ex.printStackTrace();
					}
				} else {
					LockSupport.parkNanos(waitUS * 1000);
				}

				if (tries+1 == maxTries) {
					throw new OutOfMemoryException(String.format("Request failed after %d tries: " + e.getMessage(), tries));
				}
			} catch (TTransportException e) {
				System.out.println("==> Thrift transport exception: " + e.getType());
				throw new KawkabException(e);
			} catch (TException e) {
				if (e.getMessage().contains("Unable to create the file")) {
					System.out.println("*");
					continue;
				} else if (e.getMessage().contains("Invalid timestamps")) {
					System.out.println("ILA");
					continue;
				}
				throw new KawkabException(e);
			}
		}

		throw new KawkabException(String.format("Request failed after %d tries: ", tries));
	}

	public void appendRecordsAsync(ByteBuffer buffer, AsyncMethodCallback<Integer> resultHandler) throws OutOfMemoryException, KawkabException {
		try {
			aClient.appendRecords(buffer, resultHandler);
		} catch (TException e) {
			if (e.getMessage().contains("Unable to create the file")) {
				System.out.println("*");
			}
			throw new KawkabException(e);
		}
	}

	public int appendNoops(ByteBuffer buffer) throws OutOfMemoryException, KawkabException {
		int tries = 0;
		while(++tries < MAX_TRIES) {
			try {
				return sClient.appendNoops(buffer);
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
				return sClient.appendRecordBuffered(sessionID, srcBuf, recSize);
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
			return sClient.appendRecordBatched(sessionID, srcBufs, recSize);
		} catch (TTransportException e) {
			System.out.println("==> Thrift transport exception: " + e.getType());
			throw new KawkabException(e);
		} catch (TException e) {
			throw new KawkabException(e);
		}
	}

	public long size(int sessionID) throws KawkabException {
		try {
			return sClient.size(sessionID);
		} catch (TTransportException e) {
			System.out.println("==> Thrift transport exception: " + e.getType());
			throw new KawkabException(e);
		} catch (TException e) {
			throw new KawkabException(e);
		}
	}

	public int recordSize(int sessionID) throws KawkabException {
		try {
			return sClient.recordSize(sessionID);
		} catch (TTransportException e) {
			System.out.println("==> Thrift transport exception: " + e.getType());
			throw new KawkabException(e);
		} catch (TException e) {
			throw new KawkabException(e);
		}
	}

	public void close(int sessionID) throws KawkabException {
		try {
			sClient.close(sessionID);
		} catch (TTransportException e) {
			System.out.println("==> Thrift transport exception: " + e.getType());
			throw new KawkabException(e);
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
			sClient.bulkClose(idsList);
		} catch (TTransportException e) {
			System.out.println("==> Thrift transport exception: " + e.getType());
			throw new KawkabException(e);
		} catch (TException e) {
			throw new KawkabException(e);
		}
	}

	public int noopWrite(long none) throws KawkabException {
		try {
			return sClient.noopWrite(none);
		} catch (TTransportException e) {
			System.out.println("==> Thrift transport exception: " + e.getType());
			throw new KawkabException(e);
		} catch (TException e) {
			throw new KawkabException(e);
		}
	}

	public ByteBuffer noopRead(int recSize) throws KawkabException {
		try {
			return sClient.noopRead(recSize);
		} catch (TTransportException e) {
			System.out.println("==> Thrift transport exception: " + e.getType());
			throw new KawkabException(e);
		} catch (TException e) {
			throw new KawkabException(e);
		}
	}

	public void flush() {
		try {
			sClient.flush();
		} catch (TTransportException e) {
			System.out.println("==> Thrift transport exception: " + e.getType());
			e.printStackTrace();
		} catch (TException e) {
			e.printStackTrace();
		}
	}

	public void disconnect() {
		//aTransport.close();
		sTransport.close();

		//aTransport = null;
		sTransport = null;
		aClient = null;
		sClient = null;
	}

	public void printStats(int sessionID) {
		try {
			sClient.printStats(sessionID);
		} catch (TTransportException e) {
			System.out.println("==> Thrift transport exception: " + e.getType());
			e.printStackTrace();
		} catch (TException e) {
			e.printStackTrace();
		}
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
