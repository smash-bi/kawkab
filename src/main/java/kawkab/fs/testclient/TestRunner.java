package kawkab.fs.testclient;

import kawkab.fs.api.Record;
import kawkab.fs.core.exceptions.KawkabException;
import kawkab.fs.utils.Accumulator;

public class TestRunner {
	private TestClientServiceServer tserver;

	public enum TestType {
		APPEND,
		NOOP,
		READ
	}

	Accumulator[] runTest(int testID, int testDurSec, int nc, int cidOffset, int nf, String sip, int sport, int batchSize, Record recGen,
						  Printer pr, int warmupSecs, final TestType type, int totalClients, int mid, String mip, int mport, String outFile) {
		final Accumulator[] accms = new Accumulator[nc];
		Thread[] threads = new Thread[nc];
		for (int i=0; i<nc; i++) {
			final int clid = cidOffset+i;
			final int idx = i;
			threads[i] = new Thread(()->{
				try {
					TestClientServiceClient client = connectToMaster(mip, mport);
					if (clid == mid) {
						client.setup(testID);
					}

					client.barrier(clid);

					Accumulator accm = null;

					switch (type) {
						case APPEND:
							accm = runAppendTest(testDurSec, clid, nf, sip, sport, batchSize, recGen.newRecord(), pr, warmupSecs);
							break;
						case NOOP:
							accm = runNoopTest(testDurSec, clid, sip, sport, recGen.newRecord(), pr, warmupSecs);
							break;
						case READ:
						default:
							throw new KawkabException("Test not implemented " + type);
					}

					synchronized (accms) {
						accms[idx] = accm;
					}

					System.out.printf("Client %d finished\n", clid);
					//printStats(clid, accm, batchSize, recGen.size());

					client.barrier(clid);

					Result result = sync(client, clid, testID, true, accm);

					if (clid == 1) {
						System.out.println("Saving results to " + outFile);
						result.exportJson(outFile+".json", false);
						result.exportJson(outFile+"-hists.json", true);
						result.exportCsv(outFile+".csv");
					}

					disconnectFromMaster(client);
				} catch (KawkabException e) {
					e.printStackTrace();
				}
			});

			threads[i].start();
		}

		for(int i=0; i<nc; i++){
			try {
				threads[i].join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		return accms;
	}

	private Accumulator runAppendTest(final int durSec, final int cid, int nTestFiles, final String sip, final int sport,
									  int batchSize, Record recGen, final Printer pr, int warmupSecs) throws KawkabException {
		TestClient client = new TestClient(cid, sip, sport, recGen, pr);
		client.connect();
		Accumulator accm = client.runTest(durSec, nTestFiles, warmupSecs, batchSize);
		client.disconnect();

		return accm;
	}

	private Accumulator runNoopTest(final int durSec, final int cid, final String sip, final int sport,
									Record recGen, final Printer pr, int warmupSecs) throws KawkabException {
		TestClient client = new TestClient(cid, sip, sport, recGen, pr);
		client.connect();
		Accumulator accm = client.runNoopTest(durSec, warmupSecs);
		client.disconnect();

		return accm;
	}

	private TestClientServiceClient connectToMaster(String mip, int mport) throws KawkabException {
		return new TestClientServiceClient(mip, mport);
	}

	private void disconnectFromMaster(TestClientServiceClient client) {
		client.disconnect();
	}

	private Result sync(TestClientServiceClient client, int clid, int testID, boolean stopAll, Accumulator accm) {
		SyncResponse r = client.sync(clid, testID, stopAll, accm.dataTput(), accm.opsTput(), accm);
		return new Result(r.count, r.opsTput, r.tput, r.lat50, r.lat95, r.lat99, r.latMin, r.latMax, r.latMean,
				0, 0, 0, 0, 0, 0, new long[]{}, new long[]{});
	}

	public void startServer(int numClients, int svrPort) throws KawkabException {
		tserver = new TestClientServiceServer(numClients, svrPort);
		tserver.startServer();
	}

	public void stopServer() {
		assert tserver != null;

		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		tserver.stopServer();
	}

	private void printStats(int cid, Accumulator accm, int batchSize, int recSize) {
		long cnt = accm.count()*batchSize;
		double sizeMB = recSize*cnt / (1024.0 * 1024.0);

		System.out.println(String.format("TestClient %d: size=%,.2f MB, thr=%,.2f MB/s, opThr=%,.0f, Latency (us): %s.\n",
				cid, sizeMB, accm.dataTput(), accm.opsTput(), accm));
	}
}
