package kawkab.fs.core.exceptions;

public class FileHandleClosedException extends KawkabException{
	public FileHandleClosedException(){
		super();
	}

	public FileHandleClosedException(String errorMsg){
		super(errorMsg);
	}
}
