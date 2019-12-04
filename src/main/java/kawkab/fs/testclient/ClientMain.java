package kawkab.fs.testclient;

import kawkab.fs.api.Record;
import kawkab.fs.core.exceptions.KawkabException;
import kawkab.fs.records.BytesRecord;
import kawkab.fs.records.SampleRecord;
import kawkab.fs.records.SixteenRecord;
import kawkab.fs.utils.Accumulator;
import kawkab.fs.utils.GCMonitor;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import static kawkab.fs.testclient.TestRunner.*;

public class ClientMain {
	private Printer pr;

	public static void main(String[] args) throws KawkabException, InterruptedException {
		ClientMain main = new ClientMain();
		main.init(args);
	}

	private void init(String[] args) throws KawkabException, InterruptedException {
		String usage = "Usage: TestClient cid=clientID sip=svrIP sport=svrPort wt=waitTimeMs " +
				"mid=masterID mip=masterIP mport=masterPort mgc=true|false nc=numClients" +
				"bs=batchSize rs=16|42 fp=filePrefix type=apnd|noop tc=totalClients";

		if (args.length == 1) {
			args = args[0].split(" ");
		}

		if (args.length != 15) {
			System.out.printf("Expecting 15 arguments, %d given.\n%s\n", args.length, usage);
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
		int tc 		= 0; // Total number of clients
		int nc		= 0; // Number of client threads in this instance
		int bs		= 0; // batch size; number of requests per message
		int rs		= 0; // record size in bytes
		int nf		= 0; //Number of files to read/write
		String type = "apnd";
		int warmupsec = 30;
		int testDurSec = 120;
		String fp = "test-";
		int testID = 1;

		StringBuilder params = new StringBuilder();
		for (String iArg : args) {
			System.out.println(iArg);
			String[] arg = iArg.split("=");
			params.append(iArg).append("\n");
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
				case "fp": fp = arg[1]; break;
				case "typ": type = arg[1]; break;
				case "tc": tc = Integer.parseInt(arg[1]); break;
				default: System.out.printf("Invalid argument %s.\n%s",iArg,usage); return;
			}
		}
		params.append("Twup=").append(warmupsec).append("\ntdur=").append(testDurSec);

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

		pr = new Printer();
		pr.print("Starting client " + cid);

		Accumulator[] accms = runTest(testID, cid, testDurSec, nc, nf, sip, sport, bs, recGen, warmupsec, type, tc, mid, mip, mport, wtMs, fp);

		saveResults(cid, accms, fp, testDurSec, recGen.size(), nc, nf, bs);
		ClientUtils.writeToFile(params.toString(), fp+"/params.txt");

		pr.print("Finished client " + cid);
	}

	private Accumulator[] runTest(int testID, int cid, int testDurSec, int numCients, int filesPerClient,
								  String svrIP, int sport, int batchSize, Record recGen, int warmupsec, String type,
								  int totalClients, int mid, String mip, int mport, int initWaitMs, String filePrefix) throws InterruptedException, KawkabException {
		TestRunner at = new TestRunner();

		if (cid == mid) {
			at.startServer(totalClients, mport);
		}

		if (initWaitMs > 0) {
			initWait(initWaitMs);
		}

		TestType ttype = type.equals("apnd") ? TestType.APPEND : type.equals("noop")? TestType.NOOP : null;
		if (ttype == null) {
			System.out.println("Invalid test type " + type);
			return null;
		}

		String fp = filePrefix+"/results";

		Accumulator[] accms = at.runTest(testID, testDurSec, numCients, cid, filesPerClient, svrIP, sport, batchSize, recGen, pr, warmupsec, ttype,
				totalClients, mid, mip, mport, fp);

		if (cid == mid) {
			at.stopServer();
		}

		return accms;
	}

	private void saveResults(int cidOffset, Accumulator[] accms, String filePrefix, int testDurSec, int recSize, int nc, int nf, int bs) {
		for (int i=0; i<accms.length; i++) {
			Result res = ClientUtils.prepareResult(accms[i], testDurSec, recSize, nc, nf, bs, false);
			String fp = filePrefix+"/clients/client-"+(cidOffset+i);
			ClientUtils.saveResult(res, fp);
		}

		Accumulator accm = ClientUtils.merge(accms);
		Result result = ClientUtils.prepareResult(accm, testDurSec, recSize, nc, nf, bs, true);

		/*System.out.println(result.toJson(false));
		System.out.println(result.csvHeader());
		System.out.println(result.csv());*/

		ClientUtils.saveResult(result, filePrefix+"/results-"+cidOffset);
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
