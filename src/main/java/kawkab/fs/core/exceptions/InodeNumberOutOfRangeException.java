package kawkab.fs.core.exceptions;

public class InodeNumberOutOfRangeException extends KawkabException {
	public InodeNumberOutOfRangeException(){
		super();
	}
	
	public InodeNumberOutOfRangeException(String errorMsg){
		super(errorMsg);
	}
}
