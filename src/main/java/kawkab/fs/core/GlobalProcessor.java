package kawkab.fs.core;

import kawkab.fs.core.exceptions.FileNotExistException;
import kawkab.fs.core.exceptions.KawkabException;

public interface GlobalProcessor{
	public void load(Block block) throws FileNotExistException, KawkabException;

	public void store(Block block) throws KawkabException;
}
