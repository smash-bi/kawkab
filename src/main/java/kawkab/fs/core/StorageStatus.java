package kawkab.fs.core;

import java.util.concurrent.atomic.AtomicInteger;

public class StorageStatus {
	private static final int IN_LOCAL_CODE     = 1;
	private static final int NOT_IN_CACHE_CODE = 2;
	private static final int IN_GLOBAL_CODE    = 4;
	
	private AtomicInteger status;
	
	public StorageStatus() {
		status = new AtomicInteger(0);
	}
	
	public int setSavedInLocal() {
		return status.addAndGet(IN_LOCAL_CODE);
	}
	
	public int unsetSavedInLocal() {
		return status.addAndGet(-IN_LOCAL_CODE);
	}
	
	public int setCacheEvicted() {
		return status.addAndGet(NOT_IN_CACHE_CODE);
	}
	
	public int unsetCacheEvicted() {
		return status.addAndGet(-NOT_IN_CACHE_CODE);
	}
	
	public int setSavedInGlobal() {
		return status.addAndGet(IN_GLOBAL_CODE);
	}
	
	public int unsetSavedInGlobal() {
		return status.addAndGet(-IN_GLOBAL_CODE);
	}
	
	public boolean isInLocal(int status) {
		return (status & 0x1) == 1;
	}
	
	public boolean isCacheEvicted(int status) {
		return (status & 0x02) == 2;
	}
	
	public boolean isInGlobal(int status) {
		return (status & 0x04) == 4;
	}
}
