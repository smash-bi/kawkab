package kawkab.fs.core.exceptions;

public class AlreadyConfiguredException extends KawkabException {
	public AlreadyConfiguredException(){
		super();
	}
	
	public AlreadyConfiguredException(String errorMsg){
		super(errorMsg);
	}
}
