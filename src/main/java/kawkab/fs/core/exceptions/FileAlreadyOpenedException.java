package kawkab.fs.core.exceptions;

public class FileAlreadyOpenedException extends KawkabException {
	public FileAlreadyOpenedException(){
		super();
	}
	
	public FileAlreadyOpenedException(String errorMsg){
		super(errorMsg);
	}
}
