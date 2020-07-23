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
import org.apache.thrift.transport.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class PrimaryNodeServiceClient {
	private Map<Integer, PrimaryNodeService.Client> clients;
	private Map<Integer, TTransport> transports;
	private static PrimaryNodeServiceClient instance;
	private static NodesRegister nodesRegister = NodesRegister.instance();
	private final int BUFLEN = 500*1024;

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

	public ByteBuffer getSegment(DataSegmentID id, final int offset) throws FileNotExistException, IOException {
		//System.out.println("[PC] getSegment: " + id);

		try {
			return client(id.primaryNodeID()).getSegment(id.inumber(), id.blockInFile(), id.segmentInBlock(), id.recordSize(), offset);
		} catch (kawkab.fs.core.services.thrift.TFileNotExistException e) {
			throw new FileNotExistException();
		} catch (TTransportException e) {
			System.out.println("==> Thrift transport exception: " + e.getType());
			e.printStackTrace();
			throw new IOException(e);
		} catch (TException e) {
			e.printStackTrace();
			throw new IOException(e);
		}
	}

	public ByteBuffer getInodesBlock(InodesBlockID id) throws FileNotExistException, IOException {
		System.out.println("[PC] getInodesBlock: " + id);

		try {
			return client(id.primaryNodeID()).getInodesBlock(id.blockIndex());
		} catch (kawkab.fs.core.services.thrift.TFileNotExistException e) {
			throw new FileNotExistException();
		} catch (TTransportException e) {
			System.out.println("==> Thrift transport exception: " + e.getType());
			e.printStackTrace();
			throw new IOException(e);
		} catch (TException e) {
			e.printStackTrace();
			throw new IOException(e);
		}
	}

	public ByteBuffer getIndexNode(IndexNodeID id, int fromTsIdx) throws FileNotExistException, IOException {
		//System.out.println("[PC] getIndexNode: " + id);

		try {
			return client(id.primaryNodeID()).getIndexNode(id.inumber(), id.numNodeInIndexBlock(), fromTsIdx);
		} catch (kawkab.fs.core.services.thrift.TFileNotExistException e) {
			throw new FileNotExistException();
		} catch (TTransportException e) {
			System.out.println("==> Thrift transport exception: " + e.getType());
			e.printStackTrace();
			throw new IOException(e);
		} catch (TException e) {
			throw new IOException(e);
		}
	}

	private synchronized Client client(int nodeID) throws IOException {
		PrimaryNodeService.Client client = clients.get(nodeID);
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
