package kawkab.fs.core.exceptions;

public class InvalidFileOffsetException extends Exception{
	public InvalidFileOffsetException(){
		super();
	}
	
	public InvalidFileOffsetException(String errorMsg){
		super(errorMsg);
	}
}
