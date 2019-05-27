package kawkab.fs.core.exceptions;

public class OutOfMemoryException extends KawkabException{
	public OutOfMemoryException(){
		super();
	}
	
	public OutOfMemoryException(String errorMsg){
		super(errorMsg);
	}
}
