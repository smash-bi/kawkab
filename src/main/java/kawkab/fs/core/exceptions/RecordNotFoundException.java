package kawkab.fs.core.exceptions;

public class RecordNotFoundException extends Exception{
	public RecordNotFoundException(){
		super();
	}
	
	public RecordNotFoundException(String errorMsg){
		super(errorMsg);
	}
}
