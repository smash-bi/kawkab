package kawkab.fs.core;

import kawkab.fs.core.exceptions.FileNotExistException;
import kawkab.fs.core.exceptions.KawkabException;

public abstract class GlobalProcessor {
	private static GlobalProcessor instance;
	
	public abstract void load(Block block) throws FileNotExistException, KawkabException;

	public abstract void store(Block block) throws KawkabException;
	
	public static GlobalProcessor instance() {
		if (instance == null) {
			instance = S3Backend.instance();
		}
		
		return instance;
	}
}
