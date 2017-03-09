package kawkab.fs.core.exceptions;

public class InodeNumberOutOfRangeException extends Exception{
	public InodeNumberOutOfRangeException(){
		super();
	}
	
	public InodeNumberOutOfRangeException(String errorMsg){
		super(errorMsg);
	}
}
