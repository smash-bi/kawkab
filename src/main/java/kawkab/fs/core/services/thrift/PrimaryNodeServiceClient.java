package kawkab.fs.core.services.thrift;

import kawkab.fs.commons.Configuration;
import kawkab.fs.core.DataSegmentID;
import kawkab.fs.core.IndexNodeID;
import kawkab.fs.core.InodesBlockID;
import kawkab.fs.core.NodesRegister;
import kawkab.fs.core.exceptions.FileNotExistException;
import kawkab.fs.core.exceptions.KawkabException;
import kawkab.fs.core.services.thrift.PrimaryNodeService.Client;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class PrimaryNodeServiceClient {
	private Map<Integer, PrimaryNodeService.Client> clients;
	private Map<Integer, TTransport> transports;
	private static PrimaryNodeServiceClient instance;
	private static NodesRegister nodesRegister = NodesRegister.instance();

	private PrimaryNodeServiceClient() {
		clients = new HashMap<>();
		transports = new HashMap<>();
	}

	public static PrimaryNodeServiceClient instance() {
		if (instance == null) {
			instance = new PrimaryNodeServiceClient();
		}

		return instance;
	}

	public ByteBuffer getSegment(DataSegmentID id, final int offset) throws FileNotExistException, KawkabException {
		//System.out.println("[PC] getSegment: " + id);

		try {
			return client(id.primaryNodeID()).getSegment(id.inumber(), id.blockInFile(), id.segmentInBlock(), id.recordSize(), offset);
		} catch (kawkab.fs.core.services.thrift.TFileNotExistException e) {
			throw new FileNotExistException();
		} catch (TException e) {
			e.printStackTrace();
			throw new KawkabException(e);
		}
	}

	public ByteBuffer getInodesBlock(InodesBlockID id) throws FileNotExistException, KawkabException{
		//System.out.println("[PC] getInodesBlock: " + id);

		try {
			return client(id.primaryNodeID()).getInodesBlock(id.blockIndex());
		} catch (kawkab.fs.core.services.thrift.TFileNotExistException e) {
			throw new FileNotExistException();
		} catch (TException e) {
			throw new KawkabException(e);
		}
	}

	public ByteBuffer getIndexNode(IndexNodeID id, int fromTsIdx) throws FileNotExistException, KawkabException {
		//System.out.println("[PC] getIndexNode: " + id);

		try {
			return client(id.primaryNodeID()).getIndexNode(id.inumber(), id.numNodeInIndexBlock(), fromTsIdx);
		} catch (kawkab.fs.core.services.thrift.TFileNotExistException e) {
			throw new FileNotExistException();
		} catch (TException e) {
			throw new KawkabException(e);
		}
	}

	private synchronized Client client(int nodeID) throws KawkabException {
		PrimaryNodeService.Client client = clients.get(nodeID);
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

		TTransport transport;
		try {
			int port = Configuration.instance().primaryNodeServicePort;
			System.out.printf("[PNSC] Connecting to %s:%d\n",ip, port);
			transport = new TFramedTransport(new TSocket(ip, port));
			transport.open();
			client = new PrimaryNodeService.Client(new TBinaryProtocol(transport));
		} catch (TException x) {
			x.printStackTrace();
			throw new KawkabException(x);
		}

		clients.put(nodeID, client);
		transports.put(nodeID, transport);
		return client;
	}

	public void shutdown(){
		for(TTransport tp : transports.values()) {
			tp.close();
		}
	}
}
