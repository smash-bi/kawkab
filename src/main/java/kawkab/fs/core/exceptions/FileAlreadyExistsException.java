package kawkab.fs.core.exceptions;

public class FileAlreadyExistsException extends Exception {
	public FileAlreadyExistsException(){
		super();
	}
	
	public FileAlreadyExistsException(String errorMsg){
		super(errorMsg);
	}
}
