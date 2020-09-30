package kawkab.fs.testclient;

import kawkab.fs.testclient.thrift.TResult;
import kawkab.fs.testclient.thrift.TSyncResponse;
import kawkab.fs.testclient.thrift.TestClientService;
import kawkab.fs.utils.Accumulator;
import kawkab.fs.utils.AccumulatorMap;
import org.apache.thrift.TException;

import java.util.ArrayList;

public class TestClientServiceImpl implements TestClientService.Iface {
	private int received = 0;
	private final int numClients;
	private Result aggResult;
	private boolean halt;
	private long testID;
	private boolean ready;
	private int masterID;

	private final Object syncMutex = new Object();
	private final Object barMutex = new Object();

	private int barInCnt;
	private boolean entering = true;

	public TestClientServiceImpl(int numClients, int masterID) {
		this.numClients = numClients;
		this.masterID = masterID;
	}

	public Result testResult() {
		return aggResult;
	}

	@Override
	public TSyncResponse sync(int clid, int testID, boolean stopAll, TResult result) throws TException {
		synchronized (syncMutex) {
			try {
				if (this.testID != testID)
					throw new TException("The test IDs do not match: expected: " + this.testID + ", got: " + testID);

				if (!ready)
					throw new TException("Test setup is not ready. Call setup() first");

				received++;

				System.out.printf("Client sync: rcvd=%d, total=%d, clid=%d\n", received, numClients, clid);

				if (received == 1) {
					aggResult = new Result();
				}

				merge(aggResult, result);
				halt = halt || stopAll;
				result = null;

				if (received == numClients) {
					syncMutex.notify();
				} else if (clid == masterID) {
					syncMutex.wait();
				}

				/*if (clid == masterID) {
					if (received != numClients) {
						syncMutex.wait();
					}
				} else if (received == numClients) {
					syncMutex.notify();
				}*/

				TSyncResponse resp = response(aggResult, halt);

				if (clid == masterID) {
					assert received == numClients : String.format(" rcvd (%d) != (%d) numClients", received, numClients);
					ready = false;
					received = 0;
					printResults(aggResult);
				}
				return resp;
			} catch (Exception | AssertionError e) {
				e.printStackTrace();
				throw new TException(e.getMessage());
			}
		}
	}

	private TSyncResponse response(Result aggRes, boolean stopAll) {
		return new TSyncResponse(new TResult(0,0,0,0,0,0,
				new ArrayList<Integer>(), new ArrayList<Integer>(), new ArrayList<Integer>()), stopAll);

		/*if (aggRes == null) {
			return new TSyncResponse(new TResult(0,0,0,0,0,0,
					new ArrayList<Integer>(), new ArrayList<Integer>(), new ArrayList<Integer>(), new ArrayList<Integer>()), stopAll);
		}

		List<Integer> latHistKeys = Arrays.stream(aggRes.latHistKeys()).boxed().collect(Collectors.toUnmodifiableList());
		List<Integer> latHistValues = Arrays.stream(aggRes.latHistValues()).boxed().collect(Collectors.toUnmodifiableList());

		List<Integer> tputLogKeys = Arrays.stream(aggRes.tputLogKeys()).boxed().collect(Collectors.toUnmodifiableList());
		List<Integer> tputLogValues = Arrays.stream(aggRes.tputLogValues()).boxed().collect(Collectors.toUnmodifiableList());

		return new TSyncResponse(new TResult(aggRes.count(), aggRes.latMin(), aggRes.latMax(),
				aggRes.dataTput(), aggRes.opsTput(), aggRes.recsTput(), tputLogKeys, tputLogValues, latHistKeys, latHistValues), stopAll);*/
	}

	@Override
	public void setup(int testID) throws TException {
		System.out.printf("[TCSI Setting up for test %d, total clients=%d\n", testID, numClients);
		this.testID = testID;
		received = 0;
		aggResult = new Result();
		halt = false;
		ready = true;
	}

	@Override
	public void barrier(int clid) throws TException {
		try {
			synchronized (barMutex) {
				if (!entering) {
					throw new TException("Invalid barrier state, should be entering.");
				}

				barInCnt++;

				System.out.printf("Client %d at barrier, total %d / %d\n", clid, barInCnt, numClients);

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
		} catch (Exception | AssertionError e) {
			e.printStackTrace();
			throw new TException(e.getMessage());
		}
	}

	private void merge(Result dstResult, TResult tres) {
		//long[] latHist = tres.latHistogram.stream().mapToLong(i->i).toArray();
		//long[] tputLog = tres.tputLog.stream().mapToLong(i->i).toArray();

		int[] resLatHistKeys = tres.latHistKeys.stream().mapToInt(i->i).toArray();
		int[] resLatHistVals = tres.latHistValues.stream().mapToInt(i->i).toArray();
		int[] resTputLogVals = tres.tputLogValues.stream().mapToInt(i->i).toArray();

//		if (dstResult.tputLogValues() != null) {
//			System.out.print("Prev: ");
//			dstResult.printTputLog();
//		}

		AccumulatorMap latHist = new AccumulatorMap(resLatHistKeys, resLatHistVals);
		Accumulator resTputLog = new Accumulator(resTputLogVals);

//		System.out.print("New: ");
//		resTputLog.printPairs();

		dstResult.merge(new Result(tres.totalCount, tres.opsTput, tres.dataTput, tres.minVal, tres.maxVal,
				latHist, resTputLog, tres.recsTput));
//
//		System.out.print("Merged: ");
//		dstResult.printTputLog();
//		System.out.println();

	}

	private void printResults(Result result) {
		System.out.println("======== Aggregate Result =========");
		System.out.println(result.toJson(false, true));
		System.out.println(result.csvHeader());
		System.out.println(result.csv());
		System.out.println("====================================");
	}
}