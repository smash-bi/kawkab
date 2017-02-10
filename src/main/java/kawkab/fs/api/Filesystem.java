package kawkab.fs.api;

public class Filesystem {
	public enum FileMode {
		READ, APPEND
	}
	
	private static Filesystem instance;
	
	private Filesystem(){}
	
	public static Filesystem instance(){
		if (instance == null) {
			instance = new Filesystem();
		}
		
		return instance;
	}
	
	public FileHandle open(String filename, FileMode mode, FileOptions opts){
		//TODO: Validate input
		//TODO: Check if file already exists
		
		FileHandle file = new FileHandle(filename, opts);
		
		//Save file handles
		
		return file;
	}
}
