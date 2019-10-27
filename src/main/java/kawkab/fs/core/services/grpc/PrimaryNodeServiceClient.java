package kawkab.fs.core.services.grpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import kawkab.fs.commons.Configuration;
import kawkab.fs.core.*;
import kawkab.fs.core.exceptions.FileNotExistException;
import kawkab.fs.core.exceptions.KawkabException;
import kawkab.fs.core.services.grpc.PrimaryNodeGrpc.PrimaryNodeBlockingStub;
import kawkab.fs.core.services.grpc.PrimaryNodeRPC.*;
import kawkab.fs.core.services.proto.KawkabPrimaryNodeServiceEnums.KErrorCode;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public final class PrimaryNodeServiceClient {
	private static final Object initLock = new Object();
	
	private Map<Integer, PrimaryNodeBlockingStub> clients;
	private static PrimaryNodeServiceClient instance;
	private static NodesRegister nodesRegister = NodesRegister.instance();
	
	private PrimaryNodeServiceClient() {
		clients = new HashMap<Integer, PrimaryNodeBlockingStub>();
	}
	
	public static PrimaryNodeServiceClient instance() {
		if (instance == null) {
			synchronized(initLock) {
				instance = new PrimaryNodeServiceClient();
			}
		}
		
		return instance;
	}
	
	public boolean segmentExists(DataSegmentID id) throws KawkabException {
		System.out.println("[PC] segmentExists: " + id);
		
		KSegmentRequest sid = KSegmentRequest.newBuilder()
							.setInumber(id.inumber())
							.setBlockInFile(id.blockInFile())
							.setSegmentInBlock(id.segmentInBlock())
							.build();
		
		int remoteID = id.primaryNodeID();
		PrimaryNodeBlockingStub client = client(remoteID);
		return client.segmentExists(sid).getExists();
	}

	public ByteBuffer getSegment(DataSegmentID id, final int offset) throws FileNotExistException, KawkabException, IOException {
		System.out.println("[PC] getSegment: " + id);
		
		KSegmentRequest segID = KSegmentRequest.newBuilder()
				.setInumber(id.inumber())
				.setBlockInFile(id.blockInFile())
				.setSegmentInBlock(id.segmentInBlock())
				.setOffset(offset)
				.build();
		
		int remoteID = id.primaryNodeID();
		PrimaryNodeBlockingStub client = client(remoteID);
		KSegmentResponse resp = client.getSegment(segID);
		KErrorCode ec = resp.getErrorCode();
		if (ec == KErrorCode.BLOCK_NOT_EXIST) {
			throw new FileNotExistException();
		} else if (ec != KErrorCode.SUCCESS) {
			throw new KawkabException("RPC failed.");
		}
		
		return resp.getSegmentBytes().asReadOnlyByteBuffer();
	}
	
	public boolean inodeBlockExists(InodesBlockID id) throws KawkabException {
		System.out.println("[PC] inodeBlockExists: " + id);
		
		 KInodesBlockID kid = KInodesBlockID.newBuilder()
				 				.setBlockIndex(id.blockIndex())
				 				.build();
		 int remoteID = id.primaryNodeID();
		PrimaryNodeBlockingStub client = client(remoteID);
		return client.inodesBlockExists(kid).getExists();
	}
	
	public void getInodesBlock(InodesBlockID id, Block block) throws FileNotExistException, KawkabException, IOException {
		System.out.println("[PC] getInodesBlock: " + id);
		
		int retries = 10;
		Random rand = new Random();
		
		while (retries-- > 0) {
			KInodesBlockID kid = KInodesBlockID.newBuilder()
	 				.setBlockIndex(id.blockIndex())
	 				.build();
			
			int remoteID = id.primaryNodeID();
			PrimaryNodeBlockingStub client = client(remoteID);
			KInodesBlockResponse resp = client.getInodesBlock(kid);
			KErrorCode ec = resp.getErrorCode();
			if (ec == KErrorCode.BLOCK_NOT_EXIST) {
				throw new FileNotExistException();
			} else if (ec != KErrorCode.SUCCESS) {
				try {
					Thread.sleep(100+(rand.nextLong()%400));
				} catch (InterruptedException e) {
					throw new KawkabException(e);
				}
				continue;
			}
			
			ByteBuffer buf = resp.getInodesBlockBytes().asReadOnlyByteBuffer();
			buf.rewind();
			block.loadFrom(buf);
			return;
		}
		
		throw new KawkabException("RPC failed.");
	}

	public ByteBuffer getIndexNode(IndexNodeID nodeID, int fromTsIdx) throws KawkabException, IOException {
		System.out.println("[PC] getIndexNode: " + nodeID);

		int retries = 10;
		Random rand = new Random();

		while (retries-- > 0) {
			KIndexNodeRequest req = KIndexNodeRequest.newBuilder()
					.setInumber(nodeID.inumber())
					.setNodeNumInIndex(nodeID.nodeNumber())
					.setFromTsIdx(fromTsIdx)
					.build();

			int remoteID = nodeID.primaryNodeID();
			PrimaryNodeBlockingStub client = client(remoteID);
			KIndexNodeResponse resp = client.getIndexNode(req);
			KErrorCode ec = resp.getErrorCode();
			if (ec == KErrorCode.BLOCK_NOT_EXIST) {
				throw new FileNotExistException();
			} else if (ec != KErrorCode.SUCCESS) {
				try {
					Thread.sleep(100+(rand.nextLong()%400));
				} catch (InterruptedException e) {
					throw new KawkabException(e);
				}
				continue;
			}

			return resp.getIndexNodeBytes().asReadOnlyByteBuffer();
			//buf.rewind();
			//dstBlock.loadFrom(buf);
			//return buf;
		}

		throw new KawkabException("RPC failed.");
	}
	
	private synchronized PrimaryNodeBlockingStub client(int nodeID) throws KawkabException {
		PrimaryNodeBlockingStub client = clients.get(nodeID);
		if (client != null) {
			return client;
		}
		
		String ip = null;
		try {
			ip = nodesRegister.getIP(nodeID);
		} catch (KawkabException e) {
			e.printStackTrace();
			throw e;
		}
		
		ManagedChannel channel = ManagedChannelBuilder
									.forAddress(ip, Configuration.instance().primaryNodeServicePort)
									.maxInboundMessageSize(Configuration.instance().grpcClientFrameSize)
									.usePlaintext()
									.build();
		client = kawkab.fs.core.services.grpc.PrimaryNodeGrpc.newBlockingStub(channel);
		clients.put(nodeID, client);
		return client;
	}
}
