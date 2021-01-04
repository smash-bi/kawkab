package kawkab.fs.testclient;

import java.util.ArrayList;
import java.util.Arrays;

public class S3Test {
//	private final String accessKey = "kawkab";
//	private final String secretKey = "kawkabsecret";
//	private final String sip = "http://10.10.0.2:9000";

	private final String accessKey = "AKIA5C4WEOCAKUHDCGT6";
	private final String secretKey = "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";
	private final String sip = "https://s3.ca-central-1.amazonaws.com";

	private int[] objSizeMB;

	/*public void testUpload() throws InterruptedException {
		int[] objSizesMB = new int[]{1, 2, 4, 8, 16};
		int[] numCons = new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
		int testRuns = 5;

		ArrayList<String> results = new ArrayList<>();

		System.out.println("==================");
		System.out.println("      S3 Test");
		System.out.println("==================");

		for (int nCons : numCons) {
			for (int sizeMB : objSizesMB) {
				for (int iRun = 0; iRun < testRuns; iRun++) {
					int objSize = sizeMB * 1024 * 1024; //Convert to bytes
					int numObjs = getNumObjs(objSize);

					double tput = runUploadTest(objSize, numObjs, nCons);

					String res = String.format("\t\t(%d, %.0f, %d, %.0f),\n", nCons, objSize / 1024.0 / 1024.0, iRun+1, tput);
					results.add(res);

					System.out.println(Arrays.toString(results.toArray()));

					Thread.sleep(5000);
				}
			}
		}
	}*/

	public void testUpload(int clid, int objSize, int nCons, String outFolder) throws InterruptedException {
		int numObjs = getNumObjs(objSize);

		Result aggRes = runUploadTest(objSize, numObjs, nCons);

		String fp = String.format("%s/s3-results-%d", outFolder, clid);
		//int objSizeMB = objSize / (1024*1024);
		//String json = json(objSizeMB, nCons, tput);
		//String csv = csv(objSizeMB, nCons, tput);
		String json = aggRes.toJson(false, false);
		String csv = aggRes.csv();
		saveResults(json, csv, fp);

		System.out.println(json);
	}

	private void saveResults(String json, String csv, String filePrefix) {
		ClientUtils.writeToFile(json, filePrefix+".json");
		ClientUtils.writeToFile(csv, filePrefix+".csv");
	}

	private String json(int objSize, int nCons, double tput) {
		return String.format("{\"objsize\"=%d, \"ncons\"=%d, \"tput\"=%.0f}",objSize, nCons, tput);
	}

	private String csv(int objSize, int nCons, double tput) {
		return String.format("%d, %d, %.0f",objSize, nCons, tput);
	}

	private Result runUploadTest(int objSizeBytes, int numObjs, int numCons) {
		S3TestClient[] clients = connect(numCons);
		final Result[] results = new Result[numCons];

		System.out.printf("cons=%d, objMB=%.3f, numObjs=%d\n", numCons, objSizeBytes/1024.0/1024.0, numObjs);

		Thread[] threads = new Thread[numCons];
		for (int i=1; i<numCons; i++) {
			final int iCl = i;
			threads[i] = new Thread(() -> {
				results[iCl] = clients[iCl].uploadTest(objSizeBytes, numObjs);
			});

			threads[i].start();
		}

		results[0] = clients[0].uploadTest(objSizeBytes, numObjs);

		for (int i=1; i<numCons; i++) {
			try {
				threads[i].join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		Result aggRes = results[0];

		//double tputT = 0;
		for (int i=1; i<numCons; i++) {
			try {
				threads[i].join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			aggRes.merge(results[i]);
			System.out.printf("Client %d: %s\n", i, results[i].toJson(false, false));
			//tputT += tputs[i];
		}

		System.out.println("Aggregate results: " + aggRes.toJson(false, false));

		disconnect(clients);

		return aggRes;
	}

	private S3TestClient[] connect(int numCons) {
		S3TestClient[] clients = new S3TestClient[numCons];
		for (int i=0; i<clients.length; i++) {
			clients[i] = new S3TestClient(i, accessKey, secretKey, sip);
		}

		return clients;
	}

	private void disconnect(S3TestClient[] clients) {
		for (S3TestClient c : clients) {
			c.shutdown();
		}
	}

	private int getNumObjs(int objSize) {
		long d100KB = 100 * 1024;
		long d1MB = 1024 * 1024;
		long d200MB = 200 * d1MB;
		long d1GB = 1024*d1MB;
		long d2GB = 2L*1024*d1MB;

		if (objSize <= d100KB) { //100KB
			return (int)(d200MB / objSize); //200MB
		} else if (objSize <= d1MB) { //1 MB
			return (int)(d1GB / objSize); //1GB
		}

		return (int)(d2GB / objSize); //2GB

		//return 300;
	}
}
