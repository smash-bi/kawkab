package kawkab.fs.testclient;

import kawkab.fs.api.Record;
import kawkab.fs.core.exceptions.KawkabException;
import kawkab.fs.utils.Accumulator;

public class TestRunner {
	public enum TestType {
		APPEND,
		READ,
		NOOP
	}

	Accumulator[] runTest(int testDurSec, int nc, int cid, int nf, String sip, int sport, int batchSize, Record recGen, Printer pr, int warmupSecs, final TestType type) {
		final Accumulator[] accms = new Accumulator[nc];
		Thread[] threads = new Thread[nc];
		for (int i=0; i<nc; i++) {
			final int id = cid+i;
			final int idx = i;
			threads[i] = new Thread(()->{
				try {
					Accumulator accm = null;

					switch (type) {
						case APPEND:
							accm = runAppendTest(testDurSec, id, nf, sip, sport, batchSize, recGen.newRecord(), pr, warmupSecs);
							break;
						case NOOP:
							accm = runNoopTest(testDurSec, id, sip, sport, recGen.newRecord(), pr, warmupSecs);
							break;
						case READ:
						default:
							throw new KawkabException("Test not implemented " + type);
					}

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
}
