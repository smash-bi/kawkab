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
	//private static int inodesBlockSizeBytes = Configuration.instance().inodesBlockSizeBytes;
	//private static int indexNodeSizeBytes = Configuration.instance().indexNodeSizeBytes;
	private ConcurrentLinkedQueue<ByteBuffer> buffers;

	private LatHistogram segLog;

	public PrimaryNodeServiceImpl(int numWorkers) {
		int segmentSizeBytes = Configuration.instance().segmentSizeBytes;
		buffers = new ConcurrentLinkedQueue<>();
		for (int i=0; i<numWorkers; i++) {
			ByteBuffer buffer = ByteBuffer.allocate(segmentSizeBytes);
			buffers.add(buffer);
		}

		segLog = new LatHistogram(TimeUnit.MICROSECONDS, "PrimNodeSrvc segLog", 100, 100000);
	}

	@Override
	public ByteBuffer getSegment(long inumber, long blockInFile, int segmentInBlock, int recordSize, int offset) throws TFileNotExistException, TException {
		segLog.start();

		DataSegmentID id = new DataSegmentID(inumber, blockInFile, segmentInBlock, recordSize);

		ByteBuffer buffer = buffers.poll();
		if (buffer == null) buffer = ByteBuffer.allocate(segmentSizeBytes);
		buffer.clear();

		DataSegment ds = null;
		try {
			ds = (DataSegment)cache.acquireBlock(id);
			ds.storeTo(buffer, offset);
			buffer.flip();
			return buffer;
		} catch (IOException | KawkabException e) {
			e.printStackTrace();
			throw new TException(e);
		} finally {
			if (ds != null) {
				try {
					cache.releaseBlock(id);
				} catch (KawkabException e) {
					e.printStackTrace();
				}
			}

			buffers.offer(buffer);
			segLog.end();
		}
	}

	@Override
	public ByteBuffer getInodesBlock(int blockIndex) throws TFileNotExistException, TException {
		//System.out.printf("[PSI] getInodesBlock: %d\n", blockIndex);

		InodesBlockID id = new InodesBlockID(blockIndex);

		ByteBuffer buffer = buffers.poll();
		if (buffer == null) buffer = ByteBuffer.allocate(segmentSizeBytes);
		buffer.clear();

		InodesBlock block = null;

		try {
			block = (InodesBlock)cache.acquireBlock(id);
			block.storeTo(buffer);
			buffer.flip();

			return buffer;
		} catch (IOException | KawkabException e) {
			e.printStackTrace();
			throw new TException(e);
		} finally {
			if (block != null) {
				try {
					cache.releaseBlock(id);
				} catch (KawkabException e) {
					e.printStackTrace();
				}
			}

			buffers.offer(buffer);
		}
	}

	@Override
	public ByteBuffer getIndexNode(long inumber, int nodeNumInIndex, int fromTsIndex) throws TFileNotExistException, TException {
		IndexNodeID id = new IndexNodeID(inumber, nodeNumInIndex);

		ByteBuffer buffer = buffers.poll();
		if (buffer == null) buffer = ByteBuffer.allocate(segmentSizeBytes);
		buffer.clear();

		POHNode node = null;

		try {
			node = (POHNode) cache.acquireBlock(id);
			node.storeTo(buffer, fromTsIndex);
			buffer.flip();
			return buffer;
		} catch (IOException | KawkabException e) {
			e.printStackTrace();
			throw new TException(e);
		} finally {
			if (node  != null) {
				try {
					cache.releaseBlock(id);
				} catch (KawkabException e) {
					e.printStackTrace();
				}
			}

			buffers.offer(buffer);
		}
	}

	public void printStats() {
		segLog.printStats();
	}
}
