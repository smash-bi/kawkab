package kawkab.fs.core.exceptions;

public class MaxFileSizeExceededException extends Exception{
	public MaxFileSizeExceededException(){
		super();
	}
	
	public MaxFileSizeExceededException(String errorMsg){
		super(errorMsg);
	}
}
