package kawkab.fs.core.exceptions;

import java.io.IOException;

public class FileNotExistException extends IOException {
	public FileNotExistException(){
		super();
	}
	
	public FileNotExistException(String errorMsg){
		super(errorMsg);
	}
}
