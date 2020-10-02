package kawkab.fs.testclient;

import kawkab.fs.api.Record;
import kawkab.fs.core.exceptions.KawkabException;

import java.time.Instant;
import java.util.Arrays;
import java.util.concurrent.LinkedBlockingQueue;

public class TestRunnerAsync {
	private TestClientServiceServer tserver;

	void  runTest(int testID, double iat, int writeRatio, int testDurSec, int totalClients, int clientsPerMachine,
				  int cidOffset, int nf, String sip, int sport, int apBatchSize, Record recGen,
					  int warmupSecs, int mid, String mip, int mport, String outFolder,
				      double highMPS, int burstProb, int burstDurSec, boolean isSynchronous, boolean readRecent) {
		final Result[][] results = new Result[clientsPerMachine][];

		final LinkedBlockingQueue<Instant> rq = new LinkedBlockingQueue<>();

		Thread[] threads = new Thread[clientsPerMachine];
		for (int i=0; i<clientsPerMachine; i++) {
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

					Result[] res = runTest(clid, sip, sport, idx == 0, iat, writeRatio, totalClients,
							clientsPerMachine, testDurSec, nf, apBatchSize, warmupSecs, recGen, rq, client,
							highMPS, burstProb, burstDurSec, isSynchronous, readRecent);
					results[idx] = res;

					System.out.printf("Client %d finished\n", clid);
					//printStats(clid, accm, batchSize, recGen.size());

					if (res[0] != null) {
						readAgg = client.sync(clid, testID, true, res[0]);
						if (clid == mid) {
							readAgg = tserver.testResult();
						}

					}
					if (res[1] != null) {
						if (clid == mid /*&& res[0] != null*/) {
							client.setup(testID);
						}
						client.barrier(clid);
						writeAgg = client.sync(clid, testID, true, res[1]);
						if (clid == mid) {
							writeAgg = tserver.testResult();
						}

					}

					disconnectFromMaster(client);
				} catch (KawkabException e) {
					e.printStackTrace();
				}

				if (clid == mid) {
					processResults(readAgg, writeAgg, outFolder);
				}

				String fp = String.format("%s/results/client-%02d-", outFolder, clid);
				if (results[idx][0] != null) ClientUtils.saveResult(results[idx][0], fp+"reads");
				if (results[idx][1] != null) ClientUtils.saveResult(results[idx][1], fp+"writes");

				System.out.printf("Client %d exit\n", clid);
			});

			threads[i].setName("TestClient-"+clid);
			threads[i].start();
		}

		for(int i=0; i<clientsPerMachine; i++) {
			try {
				threads[i].join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	private void processResults(Result readAgg, Result writeAgg, String outFolder) {
		//assert readAgg != null || writeAgg != null;

		if (readAgg != null) {
			System.out.println("Read results:");
			System.out.printf("Reads: %s\n", Arrays.toString(readAgg.tputLogValues()));
			printStats(readAgg);
		} else {
			System.out.println("No read results.");
		}

		if (writeAgg != null) {
			System.out.println("Write results:");
			System.out.printf("Writes: %s\n", Arrays.toString(writeAgg.tputLogValues()));
			printStats(writeAgg);
		} else {
			System.out.println("No write results.");
		}

		Result mixedRes;
		if (readAgg != null && writeAgg != null) {
			mixedRes = new Result(readAgg);
			mixedRes.merge(writeAgg);

			System.out.println("Aggregate results:");
			System.out.printf("Mixed: %s\n", Arrays.toString(mixedRes.tputLogValues()));
			printStats(mixedRes);
		} else if (readAgg != null) {
			mixedRes = readAgg;
		} else if (writeAgg != null) {
			mixedRes = writeAgg;
		} else {
			System.out.println("No read and write results gathered.");
			return;
		}

		if (readAgg != null) { System.out.printf("Reads, %s", readAgg.csv()); }
		if (writeAgg != null) { System.out.printf("Writes, %s", writeAgg.csv()); }
		System.out.printf("All, %s", mixedRes.csv());

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
			saveResults(filePrefix + "all-", mixedRes);
		}
	}

	private void saveResults(String filePrefix, Result res) {
		res.exportJson(filePrefix+"results.json", false, false);
		res.exportJson(filePrefix+"results-hists.json", true, true);
		res.exportCsv(filePrefix+"results.csv");
	}

	private Result[] runTest(int cid, String sip, int sport, boolean isController, double iat, int writeRatio,
							 int totalClients, int clientsPerMachine, int testDurSec, int filesPerclient, int apBatchSize,
							 int warmupSecs, Record recGen, final LinkedBlockingQueue<Instant> rq, TestClientServiceClient rpcClient,
							 double highMPS, int burstProb, int burstDurSec, boolean isSynchrounous, boolean readRecent)
							throws KawkabException {

		TestClientAsync client = new TestClientAsync(cid);
		client.connect(sip, sport);
		Result[] res = client.runTest(isController, iat, writeRatio, totalClients, clientsPerMachine, testDurSec,
				filesPerclient, apBatchSize, warmupSecs, recGen, rq, rpcClient,
				highMPS, burstProb, burstDurSec, isSynchrounous, readRecent);
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
