package kawkab.fs.testclient;

import kawkab.fs.core.exceptions.KawkabException;
import kawkab.fs.testclient.thrift.TResult;
import kawkab.fs.testclient.thrift.TSyncResponse;
import kawkab.fs.testclient.thrift.TestClientService;
import kawkab.fs.utils.AccumulatorMap;
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
		int maxBufferLen = 10 * 1024 * 1024;
		try {
			transport = new TFastFramedTransport(new TSocket(serverIP, port), maxBufferLen, maxBufferLen);
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

		List<Integer> latHistKeys = Arrays.stream(result.latHistKeys()).boxed().collect(Collectors.toUnmodifiableList());
		List<Long> latHistValues = Arrays.stream(result.latHistValues()).boxed().collect(Collectors.toUnmodifiableList());

		List<Integer> tputLogKeys = Arrays.stream(result.tputLogKeys()).boxed().collect(Collectors.toUnmodifiableList());
		List<Long> tputLogValues = Arrays.stream(result.tputLogValues()).boxed().collect(Collectors.toUnmodifiableList());


		//List<Long> tputTimeLog = Arrays.stream(result.tputLog()).boxed().collect(Collectors.toUnmodifiableList());
		//List<Long> latKeys = Arrays.stream(result.latHist()).boxed().collect(Collectors.toUnmodifiableList());

		TResult taccm = new TResult(result.count(), result.latMin(), result.latMax(),
				result.dataTput(), result.opsTput(), result.recsTput(), tputLogKeys, tputLogValues, latHistKeys, latHistValues);

		try {
			TSyncResponse resp = client.sync(clid, testID, stopAll, taccm);
			TResult res = resp.aggResult;

			//long[] latHist = res.latHistogram.stream().mapToLong(i->i).toArray();
			//long[] tputLog = res.tputLog.stream().mapToLong(i->i).toArray();

			int[] resLatHistKeys = res.latHistKeys.stream().mapToInt(i->i).toArray();
			long[] resLatHistVals = res.latHistValues.stream().mapToLong(i->i).toArray();

			int[] resTputLogKeys = res.tputLogKeys.stream().mapToInt(i->i).toArray();
			long[] resTputLogVals = res.tputLogValues.stream().mapToLong(i->i).toArray();

			return new Result(res.totalCount, res.opsTput, res.dataTput, res.minVal, res.maxVal,
					new AccumulatorMap(resLatHistKeys, resLatHistVals), new AccumulatorMap(resTputLogKeys, resTputLogVals), res.recsTput);
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
