package kawkab.fs.testclient;

import kawkab.fs.core.exceptions.KawkabException;
import kawkab.fs.testclient.thrift.TAccumulator;
import kawkab.fs.testclient.thrift.TSyncResponse;
import kawkab.fs.testclient.thrift.TestClientService;
import kawkab.fs.utils.Accumulator;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFastFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class TestClientServiceClient {
	private TestClientService.Client client;
	private TTransport transport;

	public TestClientServiceClient(String serverIP, int port) throws KawkabException {
		System.out.printf("[TCSC] Connecting to %s:%d\n",serverIP,port);
		int maxBufferLen = 16 * 1024;
		try {
			transport = new TFastFramedTransport(new TSocket(serverIP, port));
			transport.open();

			TProtocol protocol = new TBinaryProtocol(transport);
			client = new TestClientService.Client(protocol);
		} catch (TException x) {
			x.printStackTrace();
			throw new KawkabException(x);
		}
	}

	public SyncResponse sync(int clid, int testID, boolean stopAll, double tput, double opsTput, Accumulator accm) {
		assert client != null;

		List<Long> histogram = Arrays.stream(accm.histogram()).boxed().collect(Collectors.toUnmodifiableList());
		TAccumulator taccm = new TAccumulator(histogram, accm.count(), accm.min(), accm.max());

		try {
			TSyncResponse resp = client.sync(clid, testID, stopAll, tput, opsTput, taccm);
			return new SyncResponse(resp.reqsCount, resp.opsTput, resp.tput, resp.lat50, resp.lat95, resp.lat99,
					resp.latMin, resp.latMax, resp.latMean, resp.stopAll);
		} catch (Exception | AssertionError e) {
			e.printStackTrace();
		}

		return null;
	}

	public void setup(int testID) {
		try {
			client.setup(testID);
		} catch (Exception | AssertionError e) {
			e.printStackTrace();
		}
	}

	public void barrier(int clid) {
		try {
			client.barrier(clid);
		} catch (Exception | AssertionError e) {
			e.printStackTrace();
		}
	}

	public void disconnect() {
		transport.close();

		transport = null;
		client = null;
	}
}
