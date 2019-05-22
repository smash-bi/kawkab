package kawkab.fs.core.exceptions;

public class MaxFileSizeExceededException extends KawkabException{
	public MaxFileSizeExceededException(){
		super();
	}
	
	public MaxFileSizeExceededException(String errorMsg){
		super(errorMsg);
	}
}
