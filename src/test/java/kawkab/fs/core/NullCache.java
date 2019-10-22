package kawkab.fs.core;

import kawkab.fs.core.exceptions.KawkabException;

import java.io.IOException;

public class NullCache extends Cache {
	@Override
	public Block acquireBlock(BlockID blockID) throws IOException, KawkabException {
		return blockID.newBlock();
	}

	@Override
	public void releaseBlock(BlockID blockID) throws KawkabException {
	}

	@Override
	public void flush() throws KawkabException {
	}

	@Override
	public void shutdown() throws KawkabException {
	}

	@Override
	public long size() {
		return 0;
	}
}
