package kawkab.fs.core.exceptions;

public class InvalidFileOffsetException extends KawkabException{
	public InvalidFileOffsetException(){
		super();
	}
	
	public InvalidFileOffsetException(String errorMsg){
		super(errorMsg);
	}
}
