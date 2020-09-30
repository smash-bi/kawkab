package kawkab.fs.core.services.thrift;

import kawkab.fs.commons.Configuration;
import kawkab.fs.core.*;
import kawkab.fs.core.exceptions.KawkabException;
import kawkab.fs.core.index.poh.POHNode;
import kawkab.fs.utils.LatHistogram;
import org.apache.thrift.TException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

public class PrimaryNodeServiceImpl implements PrimaryNodeService.Iface {
	private static Cache cache = Cache.instance();
	private static int segmentSizeBytes = Configuration.instance().segmentSizeBytes;
	private static int blockSizeBytes = Configuration.instance().dataBlockSizeBytes;
	//private static int inodesBlockSizeBytes = Configuration.instance().inodesBlockSizeBytes;
	//private static int indexNodeSizeBytes = Configuration.instance().indexNodeSizeBytes;
	//private ConcurrentLinkedQueue<ByteBuffer> buffers;
	//private ConcurrentLinkedQueue<ByteBuffer> blockBuffers;

	//private LatHistogram segLog;

	private static ThreadLocal<ByteBuffer> thrLocalBuf =
			ThreadLocal.withInitial(() -> ByteBuffer.allocate(Configuration.instance().maxBufferLen));

	public PrimaryNodeServiceImpl(int numWorkers) {
		/*buffers = new ConcurrentLinkedQueue<>();
		blockBuffers = new ConcurrentLinkedQueue<>();
		for (int i=0; i<numWorkers; i++) {
			buffers.add(ByteBuffer.allocate(segmentSizeBytes));
			blockBuffers.add(ByteBuffer.allocate(blockSizeBytes));
		}*/

		//segLog = new LatHistogram(TimeUnit.MICROSECONDS, "PrimNodeSrvc segLog", 100, 100000);
	}

	@Override
	public ByteBuffer getSegment(long inumber, long blockInFile, int segmentInBlock, int recordSize, int offset) throws TFileNotExistException, TException {
		//ByteBuffer buffer = blockBuffers.poll();
		//if (buffer == null) buffer = ByteBuffer.allocate(blockSizeBytes);
		//ByteBuffer buffer = ByteBuffer.allocate(segmentSizeBytes);
		ByteBuffer buffer = thrLocalBuf.get();
		buffer.clear();
		readSegment(buffer, inumber, blockInFile, segmentInBlock, recordSize, offset);
		buffer.flip();
		return buffer;
	}

	private int readSegment(ByteBuffer dstBuf, long inumber, long blockInFile, int segmentInBlock, int recordSize, int offset)
			throws TFileNotExistException, TException {
		DataSegmentID id = new DataSegmentID(inumber, blockInFile, segmentInBlock, recordSize);

		System.out.println("Get segment: " + id);

		DataSegment ds = null;
		try {
			ds = (DataSegment)cache.acquireBlock(id);
			ds.loadBlock(true);
			return ds.storeTo(dstBuf, offset);
		} catch (IOException | KawkabException e) {
			e.printStackTrace();
			throw new TException(e);
		} finally {
			if (ds != null) {
				cache.releaseBlock(id);
			}
		}
	}

	/**
	 * Read all segments between the given first and last segments in the block, inclusively.
	 * A segments are loaded fully. Last segment will not have some usable bytes, but still loaded.
	 * @param fileID
	 * @param blockInFile
	 * @param firstSegment
	 * @param lastSegment
	 * @param recordSize
	 * @return
	 * @throws TFileNotExistException
	 * @throws TException
	 */
	@Override
	public ByteBuffer bulkReadSegments(long fileID, long blockInFile, int firstSegment, int lastSegment, int recordSize)
			throws TFileNotExistException, TException {

		//ByteBuffer buffer = blockBuffers.poll();
		//if (buffer == null) buffer = ByteBuffer.allocate(blockSizeBytes);
		//ByteBuffer buffer = ByteBuffer.allocate(segmentSizeBytes*(lastSegment-firstSegment+1));
		ByteBuffer buffer = thrLocalBuf.get();
		buffer.clear();

		for(int i = firstSegment; i <= lastSegment; i++) {
			int bytesRead = readSegment(buffer, fileID, blockInFile, i, recordSize, 0);

			if (bytesRead < segmentSizeBytes) { //We have to read complete segments unless the segment is the last one in the file
				assert i == lastSegment : "Partial segment read must be from the last segment!";
				int diff = segmentSizeBytes - bytesRead;
				buffer.put(new byte[diff]); //To make sure we write clean data
			}
		}

		buffer.flip();
		return buffer;
	}

	@Override
	public ByteBuffer getInodesBlock(int blockIndex) throws TFileNotExistException, TException {
		//System.out.printf("[PSI] getInodesBlock: %d\n", blockIndex);

		InodesBlockID id = new InodesBlockID(blockIndex);

		//ByteBuffer buffer = buffers.poll();
		//if (buffer == null) buffer = ByteBuffer.allocate(segmentSizeBytes);
		//ByteBuffer buffer = ByteBuffer.allocate(segmentSizeBytes);
		ByteBuffer buffer = thrLocalBuf.get();
		buffer.clear();

		InodesBlock block = null;

		try {
			block = (InodesBlock)cache.acquireBlock(id);
			block.loadBlock(true);
			block.storeTo(buffer);
			buffer.flip();

			return buffer;
		} catch (IOException | KawkabException e) {
			e.printStackTrace();
			throw new TException(e);
		} catch (Exception | AssertionError e) {
			e.printStackTrace();
			throw new TException(e);
		} finally {
			if (block != null) {
				cache.releaseBlock(id);
			}
			//buffers.offer(buffer);
		}
	}

	@Override
	public ByteBuffer getIndexNode(long inumber, int nodeNumInIndex, int fromTsIndex) throws TFileNotExistException, TException {
		IndexNodeID id = new IndexNodeID(inumber, nodeNumInIndex);

		// System.out.printf("[PSI] getIndexNode request: inum=%d, nodeNumInIdx=%d, fromTsIdx=%d idxNodeID=%s\n",
		//		inumber, nodeNumInIndex, fromTsIndex, id);

		//ByteBuffer buffer = buffers.poll();
		//if (buffer == null) buffer = ByteBuffer.allocate(segmentSizeBytes);
		//ByteBuffer buffer = ByteBuffer.allocate(segmentSizeBytes);
		ByteBuffer buffer = thrLocalBuf.get();
		buffer.clear();

		POHNode node = null;

		try {
			node = (POHNode) cache.acquireBlock(id);
			node.loadBlock(true);
			node.storeTo(buffer, fromTsIndex);
			buffer.flip();
			return buffer;
		} catch (IOException | KawkabException e) {
			e.printStackTrace();
			throw new TException(e);
		} finally {
			if (node  != null) {
				cache.releaseBlock(id);
			}

			//buffers.offer(buffer);
		}
	}

	public void printStats() {
		//segLog.printStats();
	}

	public void resetStats() {
		//segLog.reset();
	}
}
