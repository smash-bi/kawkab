package kawkab.fs.core.exceptions;

public class AlreadyConfiguredException extends Exception {
	public AlreadyConfiguredException(){
		super();
	}
	
	public AlreadyConfiguredException(String errorMsg){
		super(errorMsg);
	}
}
