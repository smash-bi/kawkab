package kawkab.fs.api;

import java.util.UUID;

public final class FilePath {
	private final UUID uuid;
	private final String path;
	
	public FilePath(String path){
		this.path = path;
		this.uuid = UUID.fromString(path);
	}
	
	public String path(){
		return path;
	}
	
	public UUID uuid(){
		return uuid;
	}
}
