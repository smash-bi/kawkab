package kawkab.fs.core;

import kawkab.fs.core.exceptions.KawkabException;

public interface SyncCompleteListener {
	public void notifyStoreComplete(Block block, boolean successful) throws KawkabException; //May be we can add an exception as an argument to tell why the the task has failed
}
