package kawkab.fs.testclient;

import kawkab.fs.testclient.thrift.TAccumulator;
import kawkab.fs.testclient.thrift.TSyncResponse;
import kawkab.fs.testclient.thrift.TestClientService;
import kawkab.fs.utils.Accumulator;
import org.apache.thrift.TException;

public class TestClientServiceImpl implements TestClientService.Iface {
	private int received = 0;
	private final int numClients;
	private Accumulator aggAccm;
	private boolean halt;
	private double aggTput;
	private double aggOpsTput;
	private long testID;
	private boolean ready;

	private Object syncMutex = new Object();
	private Object barMutex = new Object();

	public TestClientServiceImpl(int numClients) {
		this.numClients = numClients;
	}

	@Override
	public TSyncResponse sync(int clid, int testID, boolean stopAll, double tput, double opsTput, TAccumulator accm) throws TException {
		synchronized (syncMutex) {
			try {
				if (this.testID != testID)
					throw new TException("The test IDs do not match: expected: " + this.testID + ", got: " + testID);

				if (!ready)
					throw new TException("Test setup is not ready. Call setup() first");

				received++;

				System.out.printf("Client sync: rcvd=%d, total=%d, clid=%d\n", received, numClients, clid);

				merge(aggAccm, accm);
				accm = null;
				aggTput += tput;
				aggOpsTput += opsTput;
				halt = halt || stopAll;

				if (received != numClients) {
					try {
						syncMutex.wait();
					} catch (InterruptedException e) {
						e.printStackTrace();
						throw new TException(e);
					}
				}

				received--;

				double[] lats = aggAccm.getLatencies();
				TSyncResponse resp = new TSyncResponse(aggAccm.count(), aggOpsTput, aggTput, lats[0], lats[1], lats[2],
						aggAccm.min(), aggAccm.max(), aggAccm.mean(), halt);

				if (received == 0) {
					ready = false;
					printResults(aggAccm, aggTput, aggOpsTput);
					System.out.println("[TCSI] Releasing the sync-barrier");
				}

				syncMutex.notify();
				return resp;
			} catch (Exception | AssertionError e) {
				e.printStackTrace();
				throw new TException(e.getMessage());
			}
		}
	}

	@Override
	public void setup(int testID) throws TException {
		System.out.printf("[TCSI Setting up for test %d, client=%d\n", testID, numClients);
		this.testID = testID;

		received = 0;
		aggAccm = new Accumulator();
		halt = false;
		aggTput = 0;
		aggOpsTput = 0;

		ready = true;
	}

	private int barInCnt;
	private boolean entering=true;
	@Override
	public void barrier(int clid) throws TException {
		synchronized (barMutex) {
			if (!entering) {
				throw new TException("Invalid barrier state, should be entering.");
			}

			barInCnt++;

			System.out.printf("Client %d at barrier, total %d\n", clid, barInCnt);

			if (barInCnt != numClients) {
				try {
					barMutex.wait();
				} catch (InterruptedException e) {
					throw new TException(e);
				}
			} else {
				entering = false;
				System.out.printf("Client %d releasing barrier\n", clid);
			}

			barInCnt--;

			if (entering) {
				throw new TException("Invalid barrier state, should not be entering.");
			}

			if (barInCnt == 0) {
				entering = true;
			}

			barMutex.notify();
		}
	}

	private void merge(Accumulator dstAccm, TAccumulator taccm) {
		long[] histogram = taccm.histogram.stream().mapToLong(i->i).toArray();
		dstAccm.merge(new Accumulator(histogram, taccm.totalCount, taccm.minVal, taccm.maxVal));
	}

	private void printResults(Accumulator accm, double thr, double opThr) {
		double[] lats = accm.getLatencies();

		Result result = new Result(accm.count(), opThr, thr, lats[0], lats[1], lats[2], accm.min(), accm.max(), accm.mean(),
				0, 0, 0,0, 0, 0, accm.histogram(), new long[]{});

		System.out.println("======== Aggregate Result =========");
		System.out.println(result.toJson(false));
		System.out.println(result.csvHeader());
		System.out.println(result.csv());
		System.out.println("====================================");
	}
}