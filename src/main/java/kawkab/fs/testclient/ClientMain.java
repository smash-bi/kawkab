package kawkab.fs.testclient;

import kawkab.fs.api.Record;
import kawkab.fs.core.exceptions.KawkabException;
import kawkab.fs.records.BytesRecord;
import kawkab.fs.records.SampleRecord;
import kawkab.fs.records.SixteenRecord;
import kawkab.fs.utils.Accumulator;
import kawkab.fs.utils.GCMonitor;

public class ClientMain {
	private Printer pr;

	public static void main(String[] args) throws KawkabException {
		ClientMain main = new ClientMain();
		main.init(args);
	}

	private void init(String[] args) throws KawkabException {
		String usage = "Usage: TestClient cid=clientID sip=svrIP sport=svrPort wt=waitTimeMs " +
				"mid=masterID mip=masterIP mport=masterPort mgc=true|false nc=numClients" +
				"bs=batchSize rs=16|42";

		if (args.length == 1) {
			args = args[0].split(" ");
		}

		if (args.length != 12) {
			System.out.printf("Expecting 9 arguments, %d given.\n%s\n", args.length, usage);
			return;
		}

		int cid		= 0;	// Client ID
		String sip	= "";	// server IP
		int sport	= 0;	// server port
		int wtMs	= 0;	// initial wait time in millis to start the clients at the same time
		int mid		= 0;	// master client ID
		String mip	= "";	// master client IP
		int mport	= 0;	// master client port
		boolean mgc	= false; // Should monitor and log garbage collection
		int nc		= 0; // Total number of clients
		int bs		= 0; // batch size; number of requests per message
		int rs		= 0; // record size in bytes
		int nf		= 0; //Number of files to read/write
		int warmupsec = 5;
		int testDurSec = 10;

		for (String iArg : args) {
			System.out.println(iArg);
			String[] arg = iArg.split("=");
			switch(arg[0]) {
				case "cid": cid = Integer.parseInt(arg[1]); break;
				case "sip": sip = arg[1]; break;
				case "sport": sport = Integer.parseInt(arg[1]); break;
				case "wt": wtMs = Integer.parseInt(arg[1]); break;
				case "mid": mid = Integer.parseInt(arg[1]); break;
				case "mip": mip = arg[1]; break;
				case "mport": mport = Integer.parseInt(arg[1]); break;
				case "mgc": mgc = Boolean.parseBoolean(arg[1]); break;
				case "nc": nc = Integer.parseInt(arg[1]); break;
				case "bs": bs = Integer.parseInt(arg[1]); break;
				case "rs": rs = Integer.parseInt(arg[1]); break;
				case "nf": nf = Integer.parseInt(arg[1]); break;
				default: System.out.printf("Invalid argument %s.\n%s",iArg,usage); return;
			}
		}

		Record recGen;
		if (rs == 16) {
			recGen =  new SixteenRecord();
		} else if (rs == SampleRecord.length()) {
			recGen = new SampleRecord();
		} else {
			recGen = new BytesRecord(rs);
		}

		System.out.printf("Warmup sec=%d, test sec=%d, record size=%d\n", warmupsec, testDurSec, recGen.size());

		if (mgc) {
			GCMonitor.initialize();
		}

		pr = new Printer(cid);
		pr.print("Starting client " + cid);

		if (wtMs > 0) {
			initWait(wtMs);
		}

		//Result result = runTest(10, cid, nf, sip, sport, mip, mport, pr);
		//System.out.println(result.toJson(false));

		TestRunner at = new TestRunner();
		Accumulator[] accms = at.runTest(testDurSec, nc, cid, nf, sip, sport, bs, recGen, pr, warmupsec, TestRunner.TestType.APPEND);
		//Accumulator[] accms = at.runTest(testDurSec, nc, cid, nf, sip, sport, bs, recGen, pr, warmupsec, TestRunner.TestType.NOOP);
		Accumulator accm = merge(accms);
		Result result = prepareResult(accm, testDurSec, recGen.size(), nc, nf, bs);

		System.out.println(result.toJson(false));
		System.out.println(result.csvHeader());
		System.out.println(result.csv());

		pr.print("Finished client " + cid);
	}

	private Accumulator merge(Accumulator[] accms) {
		Accumulator accm = accms[0];

		for (int i=1; i<accms.length; i++) {
			accm.merge(accms[i]);
		}

		return accm;
	}

	private Result prepareResult(Accumulator accm, int testDurSec, int recSize, int nc, int nf, int batchSize) {
		long cnt = accm.count()*batchSize;
		double sizeMB = recSize*cnt / (1024.0 * 1024.0);
		double thr = sizeMB / testDurSec;
		int opThr = (int)(cnt / testDurSec);

		pr.print(String.format("Agg: dur=%d sec, batch=%d, clnts=%d, files=%d, size=%.2f MB, thr=%,.2f MB/s, opThr=%,d, Lat(us): %s\n",
				testDurSec, batchSize, nc, nf*nc, sizeMB, thr, opThr, accm));

		double[] lats = accm.getLatencies();
		return new Result(cnt, opThr, thr, lats[0], lats[1], lats[2], accm.min(), accm.max(), accm.mean(),
				0, lats[0], 0, accm.mean(), 0, accm.max(), new long[]{}, accm.histogram());
	}

	private void initWait(int waitTime) {
		if (waitTime > 0) {
			pr.print("Waiting for "+waitTime+" msec.");
			try {
				Thread.sleep(waitTime);
			} catch (InterruptedException e) {
				e.printStackTrace();
				System.exit(1);
			}
		}
	}
}
