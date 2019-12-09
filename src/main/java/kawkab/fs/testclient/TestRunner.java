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

	Result[] runTest(int testID, int testDurSec, int nc, int cidOffset, int nf, String sip, int sport, int batchSize, Record recGen,
						  Printer pr, int warmupSecs, final TestType type, int totalClients, int mid, String mip, int mport, String outFolder) {
		final Result[] results = new Result[nc];
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

					Result res = null;

					try {
						switch (type) {
							case APPEND:
								res = runAppendTest(testDurSec, clid, nf, sip, sport, batchSize, recGen.newRecord(), pr, warmupSecs);
								break;
							case NOOP:
								res = runNoopTest(testDurSec, clid, nf, sip, sport, batchSize, recGen.newRecord(), pr, warmupSecs);
								break;
							case READ:
							default:
								throw new KawkabException("Test not implemented " + type);
						}
					} catch (Exception e) {
						e.printStackTrace();
					}

					synchronized (results) {
						results[idx] = res;
					}

					System.out.printf("Client %d finished\n", clid);
					//printStats(clid, accm, batchSize, recGen.size());

					Result aggRes = client.sync(clid, testID, true, batchSize, res, mid);

					client.barrier(clid);

					if (clid == mid) {
						System.out.println("Saving results to " + outFolder);
						aggRes.exportJson(outFolder+"results.json", false, false);
						aggRes.exportJson(outFolder+"results-hists.json", true, true);
						aggRes.exportCsv(outFolder+"results.csv");
					}

					String fp = String.format("%s/clients/client-%02d", outFolder, clid);
					ClientUtils.saveResult(res, fp);

					disconnectFromMaster(client);
				} catch (KawkabException e) {
					e.printStackTrace();
				}
			});

			threads[i].setName("TestClient-"+clid);
			threads[i].start();
		}

		for(int i=0; i<nc; i++) {
			try {
				threads[i].join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		return results;
	}

	private Result runAppendTest(final int durSec, final int cid, int nTestFiles, final String sip, final int sport,
									  int batchSize, Record recGen, final Printer pr, int warmupSecs) throws KawkabException {
		TestClient client = new TestClient(cid, sip, sport, recGen, pr);
		client.connect();
		Result res = client.runTest(durSec, nTestFiles, warmupSecs, batchSize);
		client.disconnect();

		return res;
	}

	private Result runNoopTest(final int durSec, final int cid, int nTestFiles, final String sip, final int sport,
							   int batchSize, Record recGen, final Printer pr, int warmupSecs) throws KawkabException {
		TestClient client = new TestClient(cid, sip, sport, recGen, pr);
		client.connect();
		Result res = client.runNoopWritesTest(durSec, nTestFiles, warmupSecs, batchSize);
		client.disconnect();

		return res;
	}

	private TestClientServiceClient connectToMaster(String mip, int mport) throws KawkabException {
		return new TestClientServiceClient(mip, mport);
	}

	private void disconnectFromMaster(TestClientServiceClient client) {
		client.disconnect();
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

	private void printStats(int cid, Result res, int recSize) {
		double sizeMB = recSize*res.count() / (1024.0 * 1024.0);

		System.out.println(String.format("TestClient %d: size=%,.2f MB, %s\n",
				cid, sizeMB, res.toJson(false, false)));
	}
}
