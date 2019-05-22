package kawkab.fs.core.exceptions;

/**
 * This exception is thrown when a file, which is already opened in the append mode, is opened again in the append mode  
 * @author sm3rizvi
 */
public class FileAlreadyExistsException extends Exception {
	public FileAlreadyExistsException(){
		super();
	}
	
	public FileAlreadyExistsException(String errorMsg){
		super(errorMsg);
	}
}
