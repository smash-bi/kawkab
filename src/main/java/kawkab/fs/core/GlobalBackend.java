package kawkab.fs.core;

import kawkab.fs.core.exceptions.FileNotExistException;
import kawkab.fs.core.exceptions.KawkabException;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface GlobalBackend {
	void loadFromGlobal(Block destBlock, int offset, int length) throws FileNotExistException, IOException;
	void bulkLoadFromGlobal(BlockLoader bl) throws FileNotExistException, IOException;
	void storeToGlobal(BlockID srcBlock, ByteBuffer stageBuf) throws KawkabException;
	void shutdown();
}
