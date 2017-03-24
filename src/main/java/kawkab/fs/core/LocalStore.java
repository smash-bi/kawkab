package kawkab.fs.core;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class LocalStore {
	private static LocalStore instance;
	
	private LocalStore(){}
	
	public static LocalStore instance(){
		if (instance == null){
			instance = new LocalStore();
		}
		
		return instance;
	}
	
	public void writeBlock(Block block) throws IOException {
		System.out.println("\tWriting block: " + block.localPath());
		File file = new File(block.localPath());
		File parent = file.getParentFile();
		if (!parent.exists()){
			parent.mkdirs();
		}
		
		ByteBuffer buffer = ByteBuffer.allocate(block.blockSize());
		block.toBuffer(buffer);
		
		BufferedOutputStream writer = new BufferedOutputStream(new FileOutputStream(file));
		writer.write(buffer.array());
		writer.close();
	}
	
	public void readBlock(Block block) throws IOException{
		System.out.println("\tReading block: " + block.localPath());
		
		File file = new File(block.localPath());
		byte[] bytes = new byte[block.blockSize()];
		
		BufferedInputStream reader = new BufferedInputStream(new FileInputStream(file));
		reader.read(bytes);
		reader.close();
		
		ByteBuffer buffer = ByteBuffer.wrap(bytes);
		block.fromBuffer(buffer);
	}
}
