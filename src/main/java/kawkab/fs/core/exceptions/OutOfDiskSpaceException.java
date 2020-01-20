package kawkab.fs.core.exceptions;

public class OutOfDiskSpaceException extends KawkabException{
	public OutOfDiskSpaceException(){
		super();
	}

	public OutOfDiskSpaceException(String errorMsg){
		super(errorMsg);
	}
}
