package kawkab.fs.client.services.thrift;

import java.nio.ByteBuffer;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import kawkab.fs.client.services.FilesystemService;
import kawkab.fs.client.services.InvalidArgumentException;
import kawkab.fs.client.services.InvalidSessionException;
import kawkab.fs.client.services.RequestFailedException;
import kawkab.fs.core.Filesystem.FileMode;
import kawkab.fs.core.exceptions.KawkabException;

public class TFilesystemServiceClient {
	private FilesystemService.Client client;
	private TTransport transport;
	
	public TFilesystemServiceClient(String ip, int port) {
		transport = new TSocket(ip, port);
		try {
			transport.open();
		} catch (TTransportException e) {
			e.printStackTrace(); //TODO: Handle the exception properly
			return;
		}
		 
		TProtocol protocol = new TCompactProtocol(transport);
		client = new FilesystemService.Client(protocol);
	}
	
	/**
	 * 
	 * @param filename
	 * @param mode FileMode.APPEND creates the file if it does not already exist.
	 * @return Returns a file specific ID that the caller should use in the subsequent file specific functions such as read,
	 *         append, and close.
	 * @throws KawkabException Exceptions from the server are wrapped in KawkabException
	 */
	public synchronized long open(String filename,FileMode mode) throws KawkabException {
		try {
			return client.open(filename, convertFileMode(mode));
		} catch (RequestFailedException | TException e) {
			throw new KawkabException(e);
		}
	}
	
	public synchronized ByteBuffer read(long sessionID, long offset, int length) throws KawkabException {
		try {
			return client.read(sessionID, offset, length);
		} catch (RequestFailedException | InvalidSessionException | InvalidArgumentException | TException e) {
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
		} catch (RequestFailedException | InvalidSessionException | TException e) {
			throw new KawkabException(e);
		}
	}
	
	public synchronized void close(long sessionID) {
		try {
			client.close(sessionID);
		} catch (TException e) {
			e.printStackTrace();
		}
	}
	
	public synchronized void disconnect() {
		transport.close();
	}
	
	private kawkab.fs.client.services.FileMode convertFileMode(FileMode mode){
	    switch(mode) {
	    case READ:
	      return kawkab.fs.client.services.FileMode.FM_READ;
	    case APPEND:
	      return kawkab.fs.client.services.FileMode.FM_APPEND;
	    default:
	      return null;
	    }
	}
}
