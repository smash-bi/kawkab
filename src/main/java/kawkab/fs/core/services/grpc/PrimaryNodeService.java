package kawkab.fs.core.services.grpc;

import java.io.IOException;

import com.google.protobuf.ByteString;

import io.grpc.stub.StreamObserver;
import kawkab.fs.core.*;
import kawkab.fs.core.exceptions.FileNotExistException;
import kawkab.fs.core.exceptions.KawkabException;
import kawkab.fs.core.index.poh.POHNode;
import kawkab.fs.core.services.grpc.PrimaryNodeGrpc.PrimaryNodeImplBase;
import kawkab.fs.core.services.grpc.PrimaryNodeRPC.KExistsResponse;
import kawkab.fs.core.services.grpc.PrimaryNodeRPC.KIndexNodeID;
import kawkab.fs.core.services.grpc.PrimaryNodeRPC.KIndexNodeRequest;
import kawkab.fs.core.services.grpc.PrimaryNodeRPC.KIndexNodeResponse;
import kawkab.fs.core.services.grpc.PrimaryNodeRPC.KInodesBlockID;
import kawkab.fs.core.services.grpc.PrimaryNodeRPC.KInodesBlockResponse;
import kawkab.fs.core.services.grpc.PrimaryNodeRPC.KSegmentID;
import kawkab.fs.core.services.grpc.PrimaryNodeRPC.KSegmentResponse;
import kawkab.fs.core.services.proto.KawkabPrimaryNodeServiceEnums.KErrorCode;

public final class PrimaryNodeService extends PrimaryNodeImplBase {
	private LocalStoreManager localStoreManager; //To check if a given block exists in the local storage
	private static Cache cache; //To acquire block and return to the client
	
	static {
		cache = Cache.instance();
	}
	
	public PrimaryNodeService() {
		localStoreManager = LocalStoreManager.instance();
	}
	
	@Override
	public void segmentExists(KSegmentID kid, StreamObserver<KExistsResponse> responseObserver) {
		BlockID bid = new DataSegmentID(kid.getInumber(), kid.getBlockInFile(), kid.getSegmentInBlock());
		
		//System.out.println("[PS] segmentExists: " + bid);
		
		blockExists(bid, responseObserver);
	}
	
	@Override
	public void getSegment(KSegmentID kid, StreamObserver<KSegmentResponse> responseObserver) {
		BlockID bid = new DataSegmentID(kid.getInumber(), kid.getBlockInFile(), kid.getSegmentInBlock());
		KErrorCode ec = KErrorCode.SUCCESS;
		
		//System.out.println("[PS] getSegment: " + bid);
		
		ByteString blockBytes = null;
		try {
			blockBytes = getData(bid);
			if (blockBytes == null)
				ec = KErrorCode.BLOCK_NOT_EXIST;
		} catch (IOException | KawkabException e) {
			e.printStackTrace();
			ec = KErrorCode.FAILED;
		}
		
		KSegmentResponse reply  = KSegmentResponse.newBuilder()
				.setSegmentBytes(blockBytes)
				.setErrorCode(ec)
				.build();

		responseObserver.onNext(reply);
		responseObserver.onCompleted(); //Finished dealing with the RPC
	}
	
	@Override
	public void inodesBlockExists(KInodesBlockID kid, StreamObserver<KExistsResponse> responseObserver) {
		BlockID bid = new InodesBlockID(kid.getBlockIndex());
		
		//System.out.println("[PS] inodesBlockExists: " + bid);
		
		blockExists(bid, responseObserver);
	}
	
	@Override
	public void getInodesBlock(KInodesBlockID kid, StreamObserver<KInodesBlockResponse> responseObserver) {
		BlockID bid = new InodesBlockID(kid.getBlockIndex());
		KErrorCode ec = KErrorCode.SUCCESS;
		
		//System.out.println("[PS] getInodesBlock: " + bid);
		
		ByteString blockBytes = null;
		try {
			blockBytes = getData(bid);
			if (blockBytes == null)
				ec = KErrorCode.BLOCK_NOT_EXIST;
		} catch (IOException | KawkabException e) {
			e.printStackTrace();
			ec = KErrorCode.FAILED;
		}
		
		KInodesBlockResponse reply  = KInodesBlockResponse.newBuilder()
				.setInodesBlockBytes(blockBytes)
				.setErrorCode(ec)
				.build();

		responseObserver.onNext(reply);
		responseObserver.onCompleted(); //Finished dealing with the RPC
	}
	
	private ByteString getData(BlockID bid) throws IOException, KawkabException {
		Block block = null;
		ByteString blockBytes = null;
		try {
			block = cache.acquireBlock(bid);
			block.loadBlock();
			blockBytes = block.byteString();
		} catch(FileNotExistException e) {
			e.printStackTrace();
			blockBytes = null;
		} finally {
			if (block != null) {
				try {
					cache.releaseBlock(bid);
				} catch (KawkabException e) {
					e.printStackTrace();
				}
			}
		}
		
		return blockBytes;
	}

	private void blockExists(BlockID bid, StreamObserver<KExistsResponse> responseObserver) {
		boolean exists = localStoreManager.exists(bid);
		KExistsResponse reply = KExistsResponse.newBuilder()
										.setExists(exists)
										.build();
		responseObserver.onNext(reply);
		responseObserver.onCompleted(); //Finished dealing with the RPC
	}

	@Override
	public void getIndexNode(KIndexNodeRequest req, StreamObserver<KIndexNodeResponse> responseObserver) {
		IndexNodeID bid = new IndexNodeID(req.getInumber(), req.getNodeNumInIndex());
		int fromTsIdx = req.getFromTsIdx();
		KErrorCode ec = KErrorCode.SUCCESS;

		System.out.println("[PS] getIndexNode: " + bid);

		ByteString blockBytes = null;
		try {
			blockBytes = getIndexNodeData(bid, fromTsIdx);
			if (blockBytes == null)
				ec = KErrorCode.BLOCK_NOT_EXIST;
		} catch (IOException | KawkabException e) {
			e.printStackTrace();
			ec = KErrorCode.FAILED;
		}

		KIndexNodeResponse reply = null;

		if (blockBytes == null) {
			reply = KIndexNodeResponse.newBuilder()
					.setErrorCode(ec)
					.build();
		} else {
			reply = KIndexNodeResponse.newBuilder()
					.setIndexNodeBytes(blockBytes)
					.setErrorCode(ec)
					.build();
		}

		responseObserver.onNext(reply);
		responseObserver.onCompleted(); //Finished dealing with the RPC
	}

	private ByteString getIndexNodeData(IndexNodeID nid, int fromTsIdx) throws IOException, KawkabException {
		POHNode block = null;
		ByteString blockBytes = null;
		try {
			block = (POHNode)cache.acquireBlock(nid);
			block.loadBlock();
			blockBytes = block.byteString(fromTsIdx);
		} catch(FileNotExistException e) {
			e.printStackTrace();
			blockBytes = null;
		} finally {
			if (block != null) {
				try {
					cache.releaseBlock(nid);
				} catch (KawkabException e) {
					e.printStackTrace();
				}
			}
		}

		return blockBytes;
	}
}
