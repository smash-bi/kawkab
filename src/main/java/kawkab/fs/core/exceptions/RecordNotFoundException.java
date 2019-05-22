package kawkab.fs.core.exceptions;

public class RecordNotFoundException extends KawkabException{
	public RecordNotFoundException(){
		super();
	}
	
	public RecordNotFoundException(String errorMsg){
		super(errorMsg);
	}
}
