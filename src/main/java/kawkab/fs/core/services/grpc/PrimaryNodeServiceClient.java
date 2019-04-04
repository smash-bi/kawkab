package kawkab.fs.core.services.grpc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import kawkab.fs.commons.Configuration;
import kawkab.fs.core.Block;
import kawkab.fs.core.DataSegmentID;
import kawkab.fs.core.InodesBlockID;
import kawkab.fs.core.NodesRegister;
import kawkab.fs.core.exceptions.FileNotExistException;
import kawkab.fs.core.exceptions.KawkabException;
import kawkab.fs.core.services.grpc.PrimaryNodeGrpc.PrimaryNodeBlockingStub;
import kawkab.fs.core.services.grpc.PrimaryNodeRPC.KInodesBlockID;
import kawkab.fs.core.services.grpc.PrimaryNodeRPC.KInodesBlockResponse;
import kawkab.fs.core.services.grpc.PrimaryNodeRPC.KSegmentID;
import kawkab.fs.core.services.grpc.PrimaryNodeRPC.KSegmentResponse;
import kawkab.fs.core.services.proto.KawkabPrimaryNodeServiceEnums.KErrorCode;

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
		
		KSegmentID sid = KSegmentID.newBuilder()
							.setInumber(id.inumber())
							.setBlockInFile(id.blockInFile())
							.setSegmentInBlock(id.segmentInBlock())
							.build();
		
		int remoteID = id.primaryNodeID();
		PrimaryNodeBlockingStub client = client(remoteID);
		return client.segmentExists(sid).getExists();
	}
	
	public void getSegment(DataSegmentID id, Block block) throws FileNotExistException, KawkabException, IOException {
		System.out.println("[PC] getSegment: " + id);
		
		KSegmentID segID = KSegmentID.newBuilder()
				.setInumber(id.inumber())
				.setBlockInFile(id.blockInFile())
				.setSegmentInBlock(id.segmentInBlock())
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
		
		ByteBuffer buf = resp.getSegmentBytes().asReadOnlyByteBuffer();
		buf.rewind();
		block.loadFrom(buf);
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
									.usePlaintext(true).build();
		client = PrimaryNodeGrpc.newBlockingStub(channel);
		clients.put(nodeID, client);
		return client;
	}
}
