package kawkab.fs.testclient;

import kawkab.fs.api.Record;
import kawkab.fs.client.KClient;
import kawkab.fs.core.Filesystem;
import kawkab.fs.core.exceptions.KawkabException;
import kawkab.fs.utils.Accumulator;

import java.util.Random;

public class TestClient {
	private int id;
	private String sip;
	private int sport;
	private String mip;
	private int mport;

	private KClient client;
	private Random rand;
	private Printer pr;
	private Record recGen;

	public TestClient(int id, String sip, int sport, String mip, int mport, Record recGen, Printer printer) throws KawkabException {
		this.id = id;
		this.sip = sip;
		this.sport = sport;
		this.mip = mip;
		this.mport = mport;

		this.pr = printer;

		rand = new Random();

		this.recGen = recGen;
	}

	public Accumulator runAppendTest(int testDurSec, int nTestFiles, int warmupSecs, int batchSize) throws KawkabException {
		pr.print("Starting append test...");
		
		if (client == null || !client.isConnected()) {
			throw new KawkabException("Client is not connected");
		}

		Accumulator accm = null;
		if (batchSize == 1) {
			accm = appendTest(testDurSec, nTestFiles, warmupSecs);
		} else {
			accm = appendTestBatched(testDurSec, nTestFiles, warmupSecs, batchSize);
		}

		long cnt = accm.count();
		double sizeMB = recGen.size()*cnt / (1024.0 * 1024.0);
		double thr = sizeMB / testDurSec;
		int opThr = (int)(cnt / testDurSec);

		pr.print(String.format("AppendTest: duration=%d sec, size=%.2f MB, thr=%.2f MB/s, opThr=%d, Latency (us): %s.\n",
				testDurSec, sizeMB, thr, opThr, accm));

		return accm;
	}

	private Accumulator appendTest(int testDurSec, int nTestFiles, int warmupSecs) throws KawkabException {
		String[] fnames = openFiles(nTestFiles, "append", Filesystem.FileMode.APPEND);

		pr.print(String.format("Ramp-up for %d seconds...", warmupSecs));
		appendRecs(fnames, warmupSecs, recGen.newRecord());

		pr.print("Appending records...");
		Accumulator accm = appendRecs(fnames, testDurSec, recGen.newRecord());

		pr.print(String.format("Ramp-down for %d seconds...", warmupSecs));
		appendRecs(fnames, warmupSecs, recGen.newRecord());

		closeFiles(fnames);

		return accm;
	}

	private Accumulator appendTestBatched(int testDurSec, int nTestFiles, int warmupSecs, int batchSize) throws KawkabException {
		String[] fnames = openFiles(nTestFiles, "append", Filesystem.FileMode.APPEND);

		pr.print(String.format("Ramp-up for %d seconds...", warmupSecs));
		appendRecsBatched(fnames, warmupSecs, recGen.newRecord(), batchSize);

		pr.print("Appending records...");
		Accumulator accm = appendRecsBatched(fnames, testDurSec, recGen.newRecord(), batchSize);

		pr.print(String.format("Ramp-down for %d seconds...", warmupSecs));
		appendRecsBatched(fnames, warmupSecs, recGen.newRecord(), batchSize);

		closeFiles(fnames);

		return accm;
	}

	private Accumulator appendRecsBatched(String[] fnames, int durSec, Record recGen, int batchSize) throws KawkabException {
		long now = System.currentTimeMillis();
		long et = now + durSec*1000;

		Accumulator accm = new Accumulator();
		Record[] batch = new Record[batchSize];
		for (int i=0; i<batchSize; i++) {
			batch[i] = recGen.newRecord();
		}

		while((now = System.currentTimeMillis()) < et) {
			String fname = fnames[rand.nextInt(fnames.length)];
			for (int i=0; i<batchSize; i++) {
				batch[i].timestamp(now);
			}

			long st = System.nanoTime();
			client.appendBuffered(fname, batch, recGen.size());
			long durNano = System .nanoTime()-st;

			accm.put((int)(durNano/1000));
		}

		return accm;
	}

	private Accumulator appendRecs(String[] fnames, int durSec, Record recGen) throws KawkabException {
		long now = System.currentTimeMillis();
		long et = now + durSec*1000;

		Accumulator accm = new Accumulator();
		Record rec = recGen.newRandomRecord(rand, now);

		while((now = System.currentTimeMillis()) < et) {
			rec.timestamp(now);
			String fname = fnames[rand.nextInt(fnames.length)];

			long st = System.nanoTime();
			client.append(fname, rec);
			long durNano = System.nanoTime()-st;

			accm.put((int)(durNano/1000));
		}

		return accm;
	}

	private String[] openFiles(int numFiles, String prefix, Filesystem.FileMode mode) throws KawkabException {
		assert client.isConnected();

		String[] files = new String[numFiles];

		for (int i=0; i<files.length; i++) {
			files[i] = String.format("%s-c%d-%d-%d",prefix,id,i+1,rand.nextInt(100000));
			client.open(files[i], mode, recGen.size());
		}

		return files;
	}

	private void closeFiles(String[] files) throws KawkabException {
		assert client.isConnected();

		for(String fname : files) {
			client.close(fname);
		}
	}

	public void connect() throws KawkabException {
		client = new KClient(id);
		client.connect(sip, sport);
	}

	public void disconnect() throws KawkabException {
		client.disconnect();
	}
}
