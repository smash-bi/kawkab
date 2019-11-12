package kawkab.fs.testclient;

import kawkab.fs.api.Record;
import kawkab.fs.core.exceptions.KawkabException;
import kawkab.fs.utils.Accumulator;

public class AppendTest {

	Accumulator[] runTest(int testDurSec, int nc, int cid, int nf, String sip, int sport, String mip, int mport, int batchSize, Record recGen, Printer pr) {
		final Accumulator[] accms = new Accumulator[nc];
		Thread[] threads = new Thread[nc];
		for (int i=0; i<nc; i++) {
			final int id = cid+i;
			final int idx = i;
			threads[i] = new Thread(()->{
				try {
					Accumulator accm = runTest(testDurSec, id, nf, sip, sport, mip, mport, batchSize, recGen.newRecord(), pr);
					synchronized (accms) {
						accms[idx] = accm;
					}
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

	private Accumulator runTest(final int durSec, final int cid, int nTestFiles, final String sip, final int sport,
								final String mip, final int mport, int batchSize, Record recGen, final Printer pr) throws KawkabException {
		TestClient client = new TestClient(cid, sip, sport, mip, mport, recGen, pr);
		client.connect();
		Accumulator accm = client.runAppendTest(durSec, nTestFiles, 5, batchSize);
		client.disconnect();

		return accm;
	}
}
