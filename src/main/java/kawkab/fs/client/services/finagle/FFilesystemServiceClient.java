package kawkab.fs.client.services.finagle;

import java.nio.ByteBuffer;

import com.twitter.finagle.Thrift;
import com.twitter.finagle.thrift.Protocols.TFinagleBinaryProtocol;
import com.twitter.util.Await;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import com.twitter.util.TimeoutException;

import kawkab.fs.client.services.FilesystemService;
import kawkab.fs.client.services.FilesystemService.ServiceIface;
import kawkab.fs.core.Filesystem.FileMode;
import kawkab.fs.core.exceptions.KawkabException;

public class FFilesystemServiceClient {
	private FilesystemService.ServiceIface client;
	
	/**
	 * @param address "ip:port" of the server to connect
	 */
	public FFilesystemServiceClient(String address) {
		client = Thrift.client()
					.withProtocolFactory(new TFinagleBinaryProtocol.Factory())
					.build(address, FilesystemService.ServiceIface.class);
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
		OpenRequest openRequest = new OpenRequest(client, filename, mode);
		
		Future<Long> reqFuture = openRequest.sendRequest();
		
		/*try {
			Await.ready(reqFuture);
		} catch (TimeoutException | InterruptedException e) {
			throw new KawkabException(e.getMessage());
		}*/
		
		/*if (openRequest.failed()) {
			throw openRequest.failureCause();
		}
		
		return openRequest.result();*/
		return openRequest.awaitResult();
	}
	
	public synchronized ByteBuffer read(long sessionID, long offset, int length) throws KawkabException {
		ReadRequest req = new ReadRequest(client, sessionID, offset, length);
		
		Future<ByteBuffer> readReqFuture = req.sendRequest();
		
		/*try {
			Await.ready(readReqFuture);
		} catch (TimeoutException | InterruptedException e) {
			throw new KawkabException(e);
		}*/
		
		/*if (req.failed()) {
			throw req.failureCause();
		}
		
		return req.result();*/
		return req.awaitResult();
	}
	
	/**
	 * @param sessionID
	 * @param buffer
	 * @return Number of bytes appended
	 * @throws KawkabException
	 */
	public synchronized int append(long sessionID, ByteBuffer buffer) throws KawkabException {
		AppendRequest req = new AppendRequest(client, sessionID, buffer);
		Future<Integer> reqFuture = req.sendRequest();
		
		/*try {
			Await.ready(reqFuture);
		} catch (TimeoutException | InterruptedException e) {
			throw new KawkabException(e);
		}*/
		
		/*if (req.failed()) {
			throw req.failureCause();
		}
		
		return req.result();*/
		return req.awaitResult();
	}
	
	public synchronized void close(long sessionID) {
		try {
			Await.ready(client.close(sessionID));
		} catch (TimeoutException | InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public synchronized void disconnect() {
		// TODO
		// Couldn't find a disconnect/close function for the Scrooge and Finagle clients
		//client.release();
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
	
	/* To send synchronous requests to the server. Call sendRequest() to send the request, which returns a Future<T> f.
	 * The caller can wait for the request to complete by calling Await.ready(f).
	 */
	private abstract class FinagleRequest<T> implements FutureEventListener<T> {
		private T result;
		private Throwable cause;
		private boolean failed;
		private boolean done;
		protected final ServiceIface client;
		
		FinagleRequest(ServiceIface client) {
			this.client = client;
		}
		
		@Override
		public synchronized void onFailure(Throwable failCause) {
			cause = failCause;
			failed = true;
			done = true;
			notifyAll();
		}

		@Override
		public synchronized void onSuccess(T value) {
			result = value;
			done = true;
			notifyAll();
		}
		
		public boolean isDone() {
			return done;
		}
		
		public T result() {
			return result;
		}
		
		public KawkabException failureCause() {
			return new KawkabException(cause);
		}
		
		public boolean failed() {
			return failed;
		}
		
		public final Future<T> sendRequest() {
			Future<T> future = sendInternal();
			future.addEventListener(this);
			return future;
		}
		
		public final synchronized T awaitResult() throws KawkabException {
			while (!done) {
				try {
					wait();
				} catch (InterruptedException e) {
					throw new KawkabException(e.getMessage());
				}
			}
			
			if (failed) {
				System.out.println("Reqest failed: " + cause.getMessage());
				throw new KawkabException(cause.getMessage());
			}
			
			return result;
		}
		
		//External objects should not call this function directly. Instead, they should call sendRequest() function.
		abstract Future<T> sendInternal();
	}
	
	private final class OpenRequest extends FinagleRequest<Long> {
		private final String filename;
		private final FileMode mode;
		
		public OpenRequest(ServiceIface client, String filename, FileMode mode) {
			super(client);
			this.filename = filename;
			this.mode = mode;
		}
		
		@Override
		Future<Long> sendInternal() {
			return client.open(filename, convertFileMode(mode));
		}
	}
	
	private final class ReadRequest extends FinagleRequest<ByteBuffer> {
		private final long sessionID;
		private final long offset;
		private final int length;
		
		public ReadRequest(ServiceIface client, long sessionID, long offset, int length) {
			super(client);
			
			this.sessionID = sessionID;
			this.offset = offset;
			this.length = length;
		}
		
		@Override
		public Future<ByteBuffer> sendInternal() {
			return client.read(sessionID, offset, length);
		}
	}
	
	private final class AppendRequest extends FinagleRequest<Integer> {
		private final long sessionID;
		private final ByteBuffer buffer;
		AppendRequest(ServiceIface client, long sessionID, ByteBuffer buffer) {
			super(client);
			this.sessionID = sessionID;
			this.buffer = buffer;
		}

		@Override
		Future<Integer> sendInternal() {
			return client.append(sessionID, buffer);
		}
	}
}
