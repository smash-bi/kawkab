package kawkab.fs.testclient;

import kawkab.fs.core.exceptions.KawkabException;
import kawkab.fs.testclient.thrift.TResult;
import kawkab.fs.testclient.thrift.TSyncResponse;
import kawkab.fs.testclient.thrift.TestClientService;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
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
			transport = new TFramedTransport(new TSocket(serverIP, port));
			transport.open();

			TProtocol protocol = new TBinaryProtocol(transport);
			client = new TestClientService.Client(protocol);
		} catch (TException x) {
			x.printStackTrace();
			throw new KawkabException(x);
		}
	}

	public Result sync(int clid, int testID, boolean stopAll, Result result) {
		assert client != null;

		List<Long> histogram = Arrays.stream(result.latHist()).boxed().collect(Collectors.toUnmodifiableList());
		List<Long> tputTimeLog = Arrays.stream(result.tputLog()).boxed().collect(Collectors.toUnmodifiableList());
		TResult taccm = new TResult(histogram, result.count(), result.latMin(), result.latMax(),
				result.dataTput(), result.opsTput(), tputTimeLog);

		try {
			TSyncResponse resp = client.sync(clid, testID, stopAll, taccm);
			TResult res = resp.aggResult;

			long[] latHist = res.latHistogram.stream().mapToLong(i->i).toArray();
			long[] tputLog = res.tputLog.stream().mapToLong(i->i).toArray();
			return new Result(res.totalCount, res.opsTput, res.dataTput, res.minVal, res.maxVal, latHist, tputLog);
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
