package kawkab.fs.core.exceptions;

public class KawkabException extends Exception {
	public KawkabException(){
		super();
	}
	
	public KawkabException(Throwable w){
		super(w);
	}
	
	public KawkabException(String errorMsg){
		super(errorMsg);
	}
}
