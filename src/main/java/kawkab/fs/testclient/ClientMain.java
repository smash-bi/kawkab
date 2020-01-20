package kawkab.fs.testclient;

import kawkab.fs.api.Record;
import kawkab.fs.core.exceptions.KawkabException;
import kawkab.fs.records.BytesRecord;
import kawkab.fs.records.SampleRecord;
import kawkab.fs.records.SixteenRecord;
import kawkab.fs.utils.Accumulator;
import kawkab.fs.utils.GCMonitor;
import org.apache.commons.math3.distribution.PoissonDistribution;
import org.apache.commons.math3.distribution.ZipfDistribution;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;

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
				"bs=batchSize rs=16|50 fp=filePrefix type=apnd|noop tc=totalClients [td=tesdDurSec]";


		/*double[] exp = {1250000};
		for (double param : exp) {
			PoissonDistribution pd = new PoissonDistribution(param);
			System.out.println(param + ": " + Arrays.toString(pd.sample(1000)));
		}
		System.exit(0);*/

		if (args.length == 1) {
			args = args[0].split(" ");
		}

		if (args.length < 15) {
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
		int nf		= 0; //Total number of files across all the clients
		int td		= 300;
		String type = "apnd";
		int warmupsec = 0;
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
				case "td": td = Integer.parseInt(arg[1]); break;
				default: System.out.printf("Invalid argument %s.\n%s",iArg,usage); return;
			}
		}
		params.append("Twup=").append(warmupsec).append("\ntdur=").append(td);

		Record recGen;
		if (rs == 16) {
			recGen =  new SixteenRecord();
		} else if (rs == SampleRecord.length()) {
			recGen = new SampleRecord();
		} else {
			recGen = new BytesRecord(rs);
		}

		assert recGen.size() == rs;

		assert nf >= tc : "Total number of files must be greater than total number of clients";

		assert nf % tc == 0 : "nf % tc should be 0, i.e., files should be equally distributed";

		int nfpc = nf / tc;

		System.out.printf("Warmup sec=%d, test sec=%d, record size=%d\n", warmupsec, td, recGen.size());

		if (mgc) {
			GCMonitor.initialize();
		}

		pr = new Printer();
		pr.print("Starting client " + cid);

		runTest(testID, cid, td, nc, nfpc, sip, sport, bs, recGen, warmupsec, type, tc, mid, mip, mport, wtMs, fp);

		ClientUtils.writeToFile(params.toString(), fp+"/params.txt");

		pr.print("Finished client " + cid);
	}

	private Result[] runTest(int testID, int cid, int testDurSec, int numCients, int filesPerClient,
								  String svrIP, int sport, int batchSize, Record recGen, int warmupsec, String type,
								  int totalClients, int mid, String mip, int mport, int initWaitMs, String filePrefix) throws InterruptedException, KawkabException {
		TestRunner at = new TestRunner();

		if (cid == mid) {
			at.startServer(totalClients, mport);
		}

		if (initWaitMs > 0) {
			initWait(initWaitMs);
		}

		TestType ttype = type.equals("apnd") ? TestType.APPEND : type.equals("noop")? TestType.NOOP : type.equals("read") ? TestType.READ : null;
		if (ttype == null) {
			System.out.println("Invalid test type " + type);
			return null;
		}

		Result[] results = at.runTest(testID, testDurSec, numCients, cid, filesPerClient, svrIP, sport, batchSize, recGen, pr, warmupsec, ttype,
				totalClients, mid, mip, mport, filePrefix);

		if (cid == mid) {
			at.stopServer();
		}

		return results;
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
