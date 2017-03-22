package kawkab.fs.core.exceptions;

public class InvalidFileModeException extends Exception {
	public InvalidFileModeException(){
		super();
	}
	
	public InvalidFileModeException(String errorMsg){
		super(errorMsg);
	}
}
