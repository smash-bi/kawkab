package kawkab.fs.core.index;

import kawkab.fs.core.exceptions.KawkabException;

import java.io.IOException;

public interface FileIndex {
	long offsetInFile(long key) throws IOException, KawkabException;
	void append(long key, long byteOffsetInFile) throws IOException, KawkabException;
}
