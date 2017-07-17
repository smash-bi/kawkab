package kawkab.fs.core;

import java.io.IOException;

public interface SyncProcessor {
	public void load(Block block) throws IOException;
	public void store(Block block) throws IOException;
}
