package kawkab.fs.core;

import kawkab.fs.core.exceptions.KawkabException;

public interface SyncCompleteListener {
	void notifyGlobalStoreComplete(BlockID blockID, boolean successful) throws KawkabException; //May be we can add an exception as an argument to tell why the the task has failed
}
