package kawkab.fs.core;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.channels.FileChannel;

import kawkab.fs.commons.Constants;

public class GlobalProcessor implements SyncProcessor {

	private BackendStore bes;

	public GlobalProcessor() {
		bes = new BackendStore(Constants.backendStorePath);
	}

	@Override
	public void load(Block block) throws IOException {
		loadBlock(block);
	}

	@Override
	public void store(Block block) throws IOException {
		storeBlock(block);		
	}

	private void storeBlock(Block block) throws IOException {
		//Read block bytes, then store it in back end

		byte[] toBytes = new byte[block.blockSize()];
		ByteArrayOutputStream os = new ByteArrayOutputStream(toBytes.length);
		WritableByteChannel toChan = Channels.newChannel(os);	

		block.storeTo(toChan);

		bes.put(block.id().toString(),os.toByteArray());
	}

	private void loadBlock(Block block) throws IOException {
		//read block bytes from backend, then store in front
	
		byte[] fromBytes = bes.get(block.id().toString());
		InputStream is = new ByteArrayInputStream(fromBytes);
		ReadableByteChannel fromChan = Channels.newChannel(is);

		block.loadFrom(fromChan);
	}
}
