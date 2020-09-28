package kawkab.fs.core.services.thrift;

import kawkab.fs.commons.Configuration;
import kawkab.fs.core.DataSegmentID;
import kawkab.fs.core.IndexNodeID;
import kawkab.fs.core.InodesBlockID;
import kawkab.fs.core.NodesRegister;
import kawkab.fs.core.exceptions.FileNotExistException;
import kawkab.fs.core.services.thrift.PrimaryNodeService.Client;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

public class PrimaryNodeServiceClient {
	private Map<Integer, ConcurrentLinkedQueue<PrimaryNodeService.Client>> clients;
	private ConcurrentLinkedQueue<TTransport> transports;
	private static PrimaryNodeServiceClient instance;
	private static NodesRegister nodesRegister = NodesRegister.instance();
	//private final int BUFLEN = 500*1024;
	//private final int BUFLEN = 2097152; //2*1024*1024; //500*1024;
	private final int BUFLEN = Configuration.instance().maxBufferLen;

	private PrimaryNodeServiceClient() {
		clients = new HashMap<>();
		transports = new ConcurrentLinkedQueue<>();
	}

	public static PrimaryNodeServiceClient instance() {
		if (instance == null) {
			instance = new PrimaryNodeServiceClient();
		}

		return instance;
	}

	public ByteBuffer getSegment(DataSegmentID id, final int offset) throws FileNotExistException, IOException {
		//System.out.println("[PC] getSegment: " + id);
		int nodeID = id.primaryNodeID();
		Client client = acquireClient(nodeID);
		try {
			return client.getSegment(id.inumber(), id.blockInFile(), id.segmentInBlock(), id.recordSize(), offset);
		} catch (kawkab.fs.core.services.thrift.TFileNotExistException e) {
			throw new FileNotExistException();
		} catch (TTransportException e) {
			System.out.println("==> Thrift transport exception: " + e.getType());
			e.printStackTrace();
			throw new IOException(e);
		} catch (TException e) {
			e.printStackTrace();
			throw new IOException(e);
		} finally {
			releaseClient(client, nodeID);
		}
	}

	public ByteBuffer getInodesBlock(InodesBlockID id) throws FileNotExistException, IOException {
		//System.out.println("[PC] getInodesBlock: " + id);

		int nodeID = id.primaryNodeID();
		Client client = acquireClient(nodeID);
		try {
			//System.out.println("Primary node of the required block: " + id.primaryNodeID());
			return client.getInodesBlock(id.blockIndex());
		} catch (kawkab.fs.core.services.thrift.TFileNotExistException e) {
			throw new FileNotExistException();
		} catch (TTransportException e) {
			System.out.println("==> Thrift transport exception: " + e.getType());
			e.printStackTrace();
			throw new IOException(e);
		} catch (TException e) {
			e.printStackTrace();
			throw new IOException(e);
		} finally {
			releaseClient(client, nodeID);
		}
	}

	public ByteBuffer getIndexNode(IndexNodeID id, int fromTsIdx) throws FileNotExistException, IOException {
		//System.out.printf("[PNSC] getIndexNode: %s, inum=%d, nodeInIdx=%d, numNodeInIdxBlock=%d, fromTsIdx=%d\n",
		//		id, id.inumber(), id.nodeNumber(), id.numNodeInIndexBlock(), fromTsIdx);

		int nodeID = id.primaryNodeID();
		Client client = acquireClient(nodeID);

		try {
			return client.getIndexNode(id.inumber(), id.nodeNumber(), fromTsIdx);
		} catch (kawkab.fs.core.services.thrift.TFileNotExistException e) {
			throw new FileNotExistException();
		} catch (TTransportException e) {
			System.out.println("==> Thrift transport exception: " + e.getType());
			e.printStackTrace();
			throw new IOException(e);
		} catch (TException e) {
			throw new IOException(e);
		} finally {
			 releaseClient(client, nodeID);
		}
	}

	private Client acquireClient(int nodeID) throws IOException {
		ConcurrentLinkedQueue<Client> clientList = clients.get(nodeID);
		if (clientList == null) {
			clientList = new ConcurrentLinkedQueue<>();
			clients.put(nodeID, clientList);
		}
		PrimaryNodeService.Client client = clientList.poll();
		if (client != null) {
			return client;
		}

		String ip = null;
		ip = nodesRegister.getIP(nodeID);

		Configuration conf = Configuration.instance();
		TTransport transport;
		try {
			int port = conf.primaryNodeServicePort;
			System.out.printf("[PNSC] Connecting to %s:%d\n",ip, port);
			transport = new TFastFramedTransport(new TSocket(ip, port), BUFLEN, BUFLEN);
			transport.open();
			client = new PrimaryNodeService.Client(new TBinaryProtocol(transport));
		} catch (TException x) {
			x.printStackTrace();
			throw new IOException(x);
		}

		//clients.put(nodeID, client);
		transports.add(transport);
		return client;
	}

	private void
	releaseClient(Client client, int nodeID) {
		clients.get(nodeID).add(client);
	}

	public void shutdown(){
		for(TTransport tp : transports) {
			tp.close();
		}
	}
}
