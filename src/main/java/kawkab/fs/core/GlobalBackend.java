package kawkab.fs.core;

import kawkab.fs.core.exceptions.FileNotExistException;
import kawkab.fs.core.exceptions.KawkabException;

public interface GlobalBackend {
	public void loadFromGlobal(Block destBlock, int offset, int length) throws FileNotExistException, KawkabException;
	public void storeToGlobal(BlockID srcBlock) throws KawkabException;
	public void shutdown();
}
