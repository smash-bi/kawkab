package kawkab.fs.core.exceptions;

public class OutOfMemoryException extends Exception{
	public OutOfMemoryException(){
		super();
	}
	
	public OutOfMemoryException(String errorMsg){
		super(errorMsg);
	}
}
