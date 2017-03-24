package kawkab.fs.core.exceptions;

import java.io.IOException;

public class InsufficientResourcesException extends IOException{
	public InsufficientResourcesException(){
		super();
	}
	
	public InsufficientResourcesException(String errorMsg){
		super(errorMsg);
	}
}
