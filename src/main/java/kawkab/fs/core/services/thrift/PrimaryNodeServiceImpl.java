package kawkab.fs.core.services.thrift;

import kawkab.fs.commons.Configuration;
import kawkab.fs.core.Cache;
import kawkab.fs.core.DataSegment;
import kawkab.fs.core.DataSegmentID;
import kawkab.fs.core.InodesBlock;
import kawkab.fs.core.InodesBlockID;
import kawkab.fs.core.index.poh.POHNode;
import kawkab.fs.core.IndexNodeID;
import kawkab.fs.core.exceptions.KawkabException;
import kawkab.fs.core.services.thrift.TFileNotExistException;
import kawkab.fs.core.services.thrift.PrimaryNodeService;
import org.apache.thrift.TException;

import java.io.IOException;
import java.nio.ByteBuffer;

public class PrimaryNodeServiceImpl implements PrimaryNodeService.Iface {
	private static Cache cache = Cache.instance();
	private static int segmentSizeBytes = Configuration.instance().segmentSizeBytes;
	private static int inodesBlockSizeBytes = Configuration.instance().inodesBlockSizeBytes;
	private static int indexNodeSizeBytes = Configuration.instance().indexNodeSizeBytes;

	@Override
	public ByteBuffer getSegment(long inumber, long blockInFile, int segmentInBlock, int recordSize, int offset) throws TFileNotExistException, TException {
		DataSegmentID id = new DataSegmentID(inumber, blockInFile, segmentInBlock, recordSize);
		ByteBuffer buffer = ByteBuffer.allocate(segmentSizeBytes);

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
		}
	}

	@Override
	public ByteBuffer getInodesBlock(int blockIndex) throws TFileNotExistException, TException {
		System.out.printf("[PNSS] getInodesBlock: %d\n", blockIndex);

		InodesBlockID id = new InodesBlockID(blockIndex);
		ByteBuffer buffer = ByteBuffer.allocate(inodesBlockSizeBytes);

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
		}
	}

	@Override
	public ByteBuffer getIndexNode(long inumber, int nodeNumInIndex, int fromTsIndex) throws TFileNotExistException, TException {
		IndexNodeID id = new IndexNodeID(inumber, nodeNumInIndex);
		ByteBuffer buffer = ByteBuffer.allocate(indexNodeSizeBytes);

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
		}
	}
}
