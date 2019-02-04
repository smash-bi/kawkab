package kawkab.fs.client.services.thrift;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.thrift.TException;

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

public class TFilesystemServiceImpl implements FilesystemService.Iface {
	private Filesystem fs;
	private Map<Long, FileHandle> sessions;			// Opened files from the clients
	private AtomicLong counter = new AtomicLong();	//To assigns unique sessionsIDs for each file open request from the clients
	
	public TFilesystemServiceImpl (Filesystem fs) {
		this.fs = fs;
		sessions = new ConcurrentHashMap<Long, FileHandle>();
	}
	
	@Override
	public long open(String filename, FileMode fileMode) throws RequestFailedException, TException {
		FileHandle handle = null;
		try {
			handle = fs.open(filename, convertFileMode(fileMode), new FileOptions());
		} catch (Exception e) {
			throw new RequestFailedException(e.getMessage());
		}
		
		long sessionID = counter.incrementAndGet();
		
		if (sessions.putIfAbsent(sessionID, handle) != null) {
			throw new RequestFailedException("Session alredy exist");
		}
		
		return sessionID;
	}

	@Override
	public ByteBuffer read(long sessionID, long offset, int length)
			throws RequestFailedException, InvalidSessionException, InvalidArgumentException, TException {
		
		FileHandle fh = sessions.get(sessionID);
	    
	    if (fh == null) {
	      throw new InvalidSessionException("Session ID is invalid or the session does not exist.");
	    }
	    
	    if (length > Constants.MAX_BUFFER_LEN) {
	    	throw new InvalidArgumentException("Maximum allowed length in bytes is "+Constants.MAX_BUFFER_LEN);
	    }
	    
	    long size;
		try {
			size = fh.size();
		} catch (KawkabException | InterruptedException e) {
			e.printStackTrace();
			throw new RequestFailedException(e.getMessage());
		}
		
	    if (offset+length > size) {
	    	throw new InvalidArgumentException(String.format("offset+len is greater than file size. offset=%d, length=%d, size=%d", 
	    			offset, length, size));
	    }
		
		byte[] buffer = new byte[length];
		
		try {
			fh.read(buffer, offset, length);
		} catch (IllegalArgumentException | IOException | KawkabException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new RequestFailedException(e.getMessage());
		}
		
		return ByteBuffer.wrap(buffer);
	}

	@Override
	public int append(long sessionID, ByteBuffer buffer)
			throws RequestFailedException, InvalidSessionException, TException {
		FileHandle fh = sessions.get(sessionID);
	    
	    if (fh == null) {
	      throw new InvalidSessionException("Session ID is invalid or the session does not exist.");
	    }
	    
	    int offset = buffer.position();
		int length = buffer.remaining();
		try {
			return fh.append(buffer.array(), offset, length);
		} catch (Exception e) {
			e.printStackTrace();
			throw new RequestFailedException(e.getMessage());
		}
	}

	@Override
	public void close(long sessionID) throws TException {
		//TODO: Close the file
		sessions.remove(sessionID);
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
}
