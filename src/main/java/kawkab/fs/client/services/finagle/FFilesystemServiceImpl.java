package kawkab.fs.client.services.finagle;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import com.twitter.util.ExceptionalFunction0;
import com.twitter.util.ExecutorServiceFuturePool;
import com.twitter.util.Future;
import com.twitter.util.FuturePools;

import kawkab.fs.api.FileHandle;
import kawkab.fs.api.FileOptions;
import kawkab.fs.client.services.FileMode;
import kawkab.fs.client.services.FilesystemService;
import kawkab.fs.client.services.InvalidArgumentException;
import kawkab.fs.client.services.InvalidSessionException;
import kawkab.fs.client.services.RequestFailedException;
import kawkab.fs.commons.Constants;
import kawkab.fs.core.Filesystem;
import kawkab.fs.core.exceptions.KawkabException;

public class FFilesystemServiceImpl implements FilesystemService.ServiceIface{
	private Filesystem fs;
	private Map<Long, FileHandle> sessions;			// Opened files from the clients
	private AtomicLong counter = new AtomicLong();	//To assigns unique sessionsIDs for each file open request from the clients
	private ExecutorServiceFuturePool futurePool;
	
	public FFilesystemServiceImpl(Filesystem fs) {
		this.fs = fs;
		sessions = new ConcurrentHashMap<Long, FileHandle>();
		futurePool = FuturePools.newFuturePool(Executors.newCachedThreadPool());
	}
	
	@Override
	public Future<Long> open(String filename, FileMode fileMode) {
		FileHandle handle = null;
		try {
			handle = fs.open(filename, convertFileMode(fileMode), new FileOptions());
		} catch (Exception e) {
			return Future.exception(new RequestFailedException().initCause(e));
		}
		
		long sessionID = counter.incrementAndGet();
		
		if (sessions.putIfAbsent(sessionID, handle) != null) {
			return Future.exception(new RequestFailedException("Session alredy exist"));
		}
		
		return Future.value(sessionID);
	}

	@Override
	public Future<ByteBuffer> read(long sessionID, long offset, int length) {
	    FileHandle fh = sessions.get(sessionID);
	    
	    if (fh == null) {
	      return Future.exception(new InvalidSessionException("Session ID is invalid or the session does not exist."));
	    }
	    
	    if (length > Constants.MAX_BUFFER_LEN) {
	    	return Future.exception(new InvalidArgumentException("Maximum allowed length in bytes is "+Constants.MAX_BUFFER_LEN));
	    }
	    
	    long size;
		try {
			size = fh.size();
		} catch (KawkabException | InterruptedException e) {
			e.printStackTrace();
			return Future.exception(new RequestFailedException().initCause(e));
		}
		
	    if (offset+length > size) {
	    	return Future.exception(new InvalidArgumentException(String.format("offset+len is greater than file size. offset=%d, length=%d, size=%d", 
	    			offset, length, size)));
	    }

	    ExceptionalFunction0<ByteBuffer> blockingWork = new ReadHandler(fh, offset, length);
	    
	    // Examples: https://github.com/twitter/finagle/blob/2df9b0b213fef0358ff870d414b7a3bab5d9fee1/README.md
	    // https://github.com/jghoman/finagle-java-example/blob/master/src/main/java/jghoman/Server.java
	    return futurePool.apply(blockingWork); //Performs read asynchronously
	}


	@Override
	public Future<Integer> append(long sessionID, ByteBuffer data) {
		FileHandle fh = sessions.get(sessionID);
	    
	    if (fh == null) {
	      return Future.exception(new InvalidSessionException("Session ID is invalid or the session does not exist."));
	    }
	    
	    ExceptionalFunction0<Integer> blockingWork = new AppendHandler(fh, data);
	    return futurePool.apply(blockingWork); //Performs append asynchronously
	}
	
	@Override
	public Future<Void> close(long sessionID) {
		//TODO: Close the file
		sessions.remove(sessionID);
		
		return Future.Void();
	}
	
	/**
	 * @param mode
	 * @return
	 */
	private Filesystem.FileMode convertFileMode(FileMode mode){
	    switch(mode) {
	    case FM_READ:
	      return Filesystem.FileMode.READ;
	    case FM_APPEND:
	      return Filesystem.FileMode.APPEND;
	    default:
	      return null;
	    }
	}
	
	private  class ReadHandler extends ExceptionalFunction0<ByteBuffer> {
		private final long offset;
		private final int length;
		private final FileHandle fh;
		
		ReadHandler(FileHandle fh, long offset, int length) {
			this.fh = fh;
			this.offset = offset;
			this.length = length;
		}

		@Override
		public ByteBuffer applyE() throws Throwable {
			// long sTime = System.currentTimeMillis();
			
			byte[] buffer = new byte[length];
			
			try {
				fh.read(buffer, offset, length);
			} catch (IllegalArgumentException | IOException | KawkabException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				throw new RequestFailedException().initCause(e);
			}
			
			return ByteBuffer.wrap(buffer);
			
			// ByteBuffer retBuf = ByteBuffer.wrap(buffer);
			// long elapsed = System.currentTimeMillis() - sTime;
			// retBuf.putLong(0, elapsed); //TODO: This is for debugging purposes. Remove this line.
			// retBuf.rewind();
			// return retBuf;
		}
	}
	
	private  class AppendHandler extends ExceptionalFunction0<Integer> {
		private final ByteBuffer buffer;
		private final FileHandle fh;
		
		AppendHandler(FileHandle fh, ByteBuffer buffer) {
			this.fh = fh;
			this.buffer = buffer;
		}

		@Override
		public Integer applyE() throws Throwable {
			int offset = buffer.position();
			int length = buffer.remaining();
			try {
				return fh.append(buffer.array(), offset, length);
			} catch (Exception e) {
				e.printStackTrace();
				throw new RequestFailedException().initCause(e);
			}
		}
	}
}
