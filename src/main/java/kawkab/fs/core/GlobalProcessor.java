package kawkab.fs.core;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
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
		ByteBuffer storeBuffer = ByteBuffer.allocate(block.blockSize());

		try(RandomAccessFile file = new RandomAccessFile(block.localPath(), "r")){
			try(FileChannel channel = file.getChannel()) {
				block.storeTo(channel);
				channel.read(storeBuffer);		
				bes.put(block.id().toString(),storeBuffer.array());
			}
		}
	}

	private void loadBlock(Block block) throws IOException {
		//read block bytes from backend, then store in front

		ByteBuffer loadBuffer = ByteBuffer.wrap(bes.get(block.id().toString()));

		try(RandomAccessFile file = new RandomAccessFile(block.localPath(), "rw")){
			try(FileChannel channel = file.getChannel()) {
				channel.write(loadBuffer);
				block.loadFrom(channel);
			}
		}
	}
}
