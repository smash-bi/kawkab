package kawkab.fs.records;

import org.joda.time.Instant;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

public class RecordsAnalyzer {
	public static void main(String[] args) throws IOException {
		String fn = "/home/sm3rizvi/kawkab/dataset/smash-data-1";
		RecordsAnalyzer ra = new RecordsAnalyzer();
		ra.readFile(fn);
	}

	public void readFile(String fn) throws IOException {
		List<Long> ts = new ArrayList<>();

		try(FileChannel channel = new RandomAccessFile(fn, "r").getChannel()) {
			long size = channel.size();

			//MappedByteBuffer map = channel.map(FileChannel.MapMode.READ_ONLY, 0, size);
			//map.order(ByteOrder.LITTLE_ENDIAN);
			//ByteBuffer buffer = map.asReadOnlyBuffer();

			ByteBuffer buffer = ByteBuffer.allocate(121);
			while(channel.read(buffer) > 0) {
				buffer.flip();
				buffer.order(ByteOrder.LITTLE_ENDIAN);

				System.out.println("Num fields " + buffer.getShort(0));
				long t = buffer.getLong(24);
				System.out.println("TS = " + new Instant(t).toDateTime());
				ts.add(t);
				buffer.clear();
			}
		}

		System.out.println("Vals = " + ts.size());

	}
}
