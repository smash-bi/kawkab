package kawkab.fs.core;

import kawkab.fs.commons.Commons;
import kawkab.fs.commons.Configuration;
import kawkab.fs.core.exceptions.FileNotExistException;
import kawkab.fs.core.services.thrift.PrimaryNodeServiceClient;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.LinkedList;

public class BlockLoader {
	private final long fileID;
	private final long blockInFile;
	private LinkedList<DataSegment> segs;
	private final static GlobalStoreManager globalStoreManager = GlobalStoreManager.instance(); // Backend store such as S3
	private int last = -1;
	protected final static PrimaryNodeServiceClient primaryNode = PrimaryNodeServiceClient.instance();
	private static final GlobalStoreManager globalStore = GlobalStoreManager.instance();
	private static int segmentSizeBytes = Configuration.instance().segmentSizeBytes;

	public BlockLoader(long fileID, long blockInFile) {
		this.fileID = fileID;
		this.blockInFile = blockInFile;
		segs = new LinkedList<>();
	}

	/**
	 * Pre-condition: All segments should be added in descending order and without any gaps
	 * @param ds
	 */
	public void add(DataSegment ds) {
		assert ds.fileID() == fileID;
		assert ds.blockInFile() == blockInFile;

		segs.addFirst(ds);
		int sib = ds.segmentInBlock();
		if (last == -1) {
			last = sib;
			return;
		}

		if (sib != last-1) {
			System.out.printf("[BL] Error: BL %d Segments out of order. Last=%d, current=%d\n", blockInFile, last, sib);
		}

		last = sib;
	}

	public int perBlockTypeKey() {
		return segs.peekFirst().id().perBlockTypeKey();
	}

	public int offset() {
		DataSegment first = segs.peekFirst();
		return first.segmentInBlock() * DataSegment.SEGMENT_SIZE_BYTES;
	}

	public int length() {
		DataSegment last = segs.peekLast();
		int len = (last.segmentInBlock() * DataSegment.SEGMENT_SIZE_BYTES + DataSegment.SEGMENT_SIZE_BYTES) - offset();

		if (len != segs.size()*DataSegment.SEGMENT_SIZE_BYTES) {
			String.format("[BL] Block %d invalid length calculation. Length %d should be equal to the length of all segments to load.\n",
					blockInFile, len);
		}

		return len;
	}

	public String blockPath() {
		return segs.peekFirst().id().localPath();
	}

	public int loadFrom(ReadableByteChannel channel)  throws IOException {
		int bytesLoaded = 0;

		int toLoad = segs.size();
		int cnt = 0;
		int last = -1;
		for (DataSegment seg : segs) {
			if (last == -1) {
				last = seg.segmentInBlock();
			} else {
				if (last+1 != seg.segmentInBlock())
					String.format("[BL] Error: BL %d segs being loaded out of order. Last=%d, current=%d\n", blockInFile, last, seg.segmentInBlock());
				last = seg.segmentInBlock();
			}

			int n = seg.loadFromWithSkips(channel);
			bytesLoaded += n;
			cnt++;
			if (n < DataSegment.SEGMENT_SIZE_BYTES)
				System.out.printf("[BL] Block %d loaded less than a seg bytes: Seg %d, bytes=%d, total=%d\n",blockInFile, seg.segmentInBlock(), n, bytesLoaded);
		}

		if (bytesLoaded < length()) {
			System.out.printf("[BL] Error: Loaded %d not loaded all bytes. Loaded %d, expected %d\n",blockInFile, bytesLoaded, length());
		}

		if (cnt != toLoad) {
			System.out.printf("[BL] Error: Loaded %d not loaded all bytes. Loaded %d, expected %d\n",blockInFile, bytesLoaded, length());
		}

		return bytesLoaded;
	}

	public void printSegsInLoader() {
		StringBuilder s = new StringBuilder();
		s.append("[BL] Block: ").append(blockInFile).append(", Segments: [");
		for (DataSegment seg : segs) {
			s.append(((DataSegmentID)seg.id()).segmentInBlock()).append(", ");
		}
		s.append("]");

		System.out.println(s.toString());
	}

	public int size() {
		return segs.size();
	}

	public void load() throws IOException, FileNotExistException {
		try {
			globalStore.bulkLoad(this);
		} catch (FileNotExistException e){
			bulkLoadFromPrimary();
			//loadFromPrimary();
		}
	}

	private void loadFromPrimary() throws FileNotExistException, IOException {
		for(DataSegment ds : segs) {
			ds.loadBlock(true);
		}
	}

	private void bulkLoadFromPrimary() throws FileNotExistException, IOException {
		DataSegment fds = segs.peekFirst();
		ByteBuffer buffer = primaryNode.bulkReadSegments(Commons.primaryWriterID(fileID), fileID, blockInFile,
				fds.segmentInBlock(), segs.peekLast().segmentInBlock(), fds.recordSize());

		int pos = buffer.position();
		int initLimit = buffer.limit();
		for(DataSegment ds : segs) {
			buffer.position(pos);
			buffer.limit(pos+segmentSizeBytes);

			ds.loadFromWithSkips(buffer);

			assert pos+segmentSizeBytes <= initLimit;
			pos += segmentSizeBytes;
		}
	}
}
