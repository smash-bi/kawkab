package kawkab.fs.core.exceptions;

public class FileAlreadyOpenedException extends Exception {
	public FileAlreadyOpenedException(){
		super();
	}
	
	public FileAlreadyOpenedException(String errorMsg){
		super(errorMsg);
	}
}
