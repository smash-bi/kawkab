package kawkab.fs.core.exceptions;

public class InvalidArgumentsException extends Exception{
	public InvalidArgumentsException(){
		super();
	}
	
	public InvalidArgumentsException(String errorMsg){
		super(errorMsg);
	}
}
