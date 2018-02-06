package kawkab.fs.core.exceptions;

public final class FileNotExistException extends KawkabException {
	public FileNotExistException(){
		super();
	}
	
	public FileNotExistException(String errorMsg){
		super(errorMsg);
	}
}
