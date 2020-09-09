package kawkab.fs.testclient;

import kawkab.fs.api.Record;
import kawkab.fs.core.exceptions.KawkabException;

import java.time.Instant;
import java.util.Arrays;
import java.util.concurrent.LinkedBlockingQueue;

public class TestRunnerAsync {
	private TestClientServiceServer tserver;

	void  runTest(int testID, double iat, int writeRatio, int testDurSec, int nc, int cidOffset, int nf, String sip, int sport, int apBatchSize, Record recGen,
					  int warmupSecs, int mid, String mip, int mport, String outFolder) {
		final Result[][] results = new Result[nc][];

		final LinkedBlockingQueue<Instant> rq = new LinkedBlockingQueue<>();

		Thread[] threads = new Thread[nc];
		for (int i=0; i<nc; i++) {
			final int clid = cidOffset+i;
			final int idx = i;
			threads[i] = new Thread(()-> {
				Result readAgg = null;
				Result writeAgg = null;
				try {
					TestClientServiceClient client = connectToMaster(mip, mport);
					if (clid == mid) {
						client.setup(testID);
					}

					client.barrier(clid);

					Result[] res = runTest(clid, sip, sport, idx == 0, iat, writeRatio, testDurSec, nf, apBatchSize, warmupSecs, recGen, rq, client);
					results[idx] = res;

					System.out.printf("Client %d finished\n", clid);
					//printStats(clid, accm, batchSize, recGen.size());

					if (res[0] != null) {
						readAgg = client.sync(clid, testID, true, res[0]);
					}
					if (res[1] != null) {
						if (clid == mid /*&& res[0] != null*/) {
							client.setup(testID);
						}
						client.barrier(clid);
						writeAgg = client.sync(clid, testID, true, res[1]);
					}

					disconnectFromMaster(client);
				} catch (KawkabException e) {
					e.printStackTrace();
				}

				if (clid == mid) {
					processResults(readAgg, writeAgg, outFolder);
				}

				String fp = String.format("%s/results/client-%02d", outFolder, clid);
				if (results[idx][0] != null) ClientUtils.saveResult(results[idx][0], fp+"reads");
				if (results[idx][1] != null) ClientUtils.saveResult(results[idx][1], fp+"writes");
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
	}

	private void processResults(Result readAgg, Result writeAgg, String outFolder) {
		assert readAgg != null || writeAgg != null;

		if (readAgg != null) {
			System.out.println("Read results:");
			System.out.printf("Reads: %s\n", Arrays.toString(readAgg.tputLog()));
			printStats(readAgg);
		} else {
			System.out.println("No read results.");
		}

		if (writeAgg != null) {
			System.out.println("Write results:");
			System.out.printf("Writes: %s\n", Arrays.toString(writeAgg.tputLog()));
			printStats(writeAgg);
		} else {
			System.out.println("No write results.");
		}

		Result mixedRes = null;
		if (readAgg != null && writeAgg != null) {
			mixedRes = new Result(readAgg);
			mixedRes.merge(writeAgg);

			System.out.println("Aggregate results:");
			System.out.printf("Mixed: %s\n", Arrays.toString(mixedRes.tputLog()));
			printStats(mixedRes);
		}

		if (readAgg != null) { System.out.printf("Reads, %s", readAgg.csv()); }
		if (writeAgg != null) { System.out.printf("Writes, %s", writeAgg.csv()); }
		if (mixedRes != null) { System.out.printf("Mixed, %s", mixedRes.csv()); }

		saveResults(outFolder + "/", readAgg, writeAgg, mixedRes);
	}

	private void saveResults(String filePrefix, Result readRes, Result writeRes, Result mixedRes) {
		System.out.println("Saving results to " + filePrefix);
		if (readRes != null) {
			saveResults(filePrefix+"read-", readRes);
		}

		if (writeRes != null) {
			saveResults(filePrefix+"write-", writeRes);
		}

		if (mixedRes != null) {
			saveResults(filePrefix + "mixed-", mixedRes);
		}
	}

	private void saveResults(String filePrefix, Result res) {
		res.exportJson(filePrefix+"results.json", false, false);
		res.exportJson(filePrefix+"results-hists.json", true, true);
		res.exportCsv(filePrefix+"results.csv");
	}

	private Result[] runTest(int cid, String sip, int sport, boolean isController, double iat, int writeRatio, int testDurSec, int filesPerclient, int apBatchSize,
							 int warmupSecs, Record recGen, final LinkedBlockingQueue<Instant> rq, TestClientServiceClient rpcClient) throws KawkabException {

		TestClientAsync client = new TestClientAsync(cid);
		client.connect(sip, sport);
		Result[] res = client.runTest(isController, iat, writeRatio, testDurSec, filesPerclient, apBatchSize, warmupSecs, recGen, rq, rpcClient);
		client.disconnect();

		return res;
	}

	private TestClientServiceClient connectToMaster(String mip, int mport) throws KawkabException {
		return new TestClientServiceClient(mip, mport);
	}

	private void disconnectFromMaster(TestClientServiceClient client) {
		client.disconnect();
	}

	public void startServer(int numClients, int svrPort, int masterID) throws KawkabException {
		tserver = new TestClientServiceServer(numClients, svrPort, masterID);
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

	private void printStats(Result res) {
		System.out.println(res.toJson(false, false));
		System.out.println(res.csvHeader());
		System.out.println(res.csv());
	}
}
