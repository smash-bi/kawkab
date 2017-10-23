package kawkab.fs.core;

import java.io.IOException;
import java.util.concurrent.Future;

public interface GlobalProcessor{
	public Future<?> load(Block block) throws IOException;

	public Future<?> store(Block block) throws IOException;
}
