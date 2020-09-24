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
	public enum TestType {
		APPEND,
		NOOP,
		READ
	}

	private Printer pr;

	public static void main(String[] args) throws KawkabException, InterruptedException {
		ClientMain main = new ClientMain();
		main.init(args);
	}

	private void init(String[] args) throws KawkabException, InterruptedException {
		String usage = "Usage: TestClient cid=clientID sip=svrIP sport=svrPort " +
				"mid=masterID mip=masterIP mport=masterPort wt=waitTimeMs [mgc=true|false nc=numClients" +
				"bs=batchSize rs=16|50 fp=filePrefix type=apnd|noop tc=totalClients td=tesdDurSec " +
				"rps=reqsPerSecx1000 wr=writeRatio]";


		/*double[] exp = {1250000};
		for (double param : exp) {
			PoissonDistribution pd = new PoissonDistribution(param);
			System.out.println(param + ": " + Arrays.toString(pd.sample(1000)));
		}
		System.exit(0);*/

		if (args.length == 1) {
			args = args[0].split(" ");
		}

		if (args.length < 6) {
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
		int nf		= 0; // Number of files per client
		int td		= 300;  // Test duration in seconds
		int warmupsec = 120; // Warmup time in seconds
		String type = "apnd";
		String fp = "test-";
		int testID = 1;
		int wr = 100; //write ratio
		double iat = 100; // inter-arrival time in microseonds

		StringBuilder params = new StringBuilder();
		for (String iArg : args) {
			//System.out.println(iArg);
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
				case "wr": wr = Integer.parseInt(arg[1]); break;
				case "iat": iat = Double.parseDouble(arg[1]); break;
				case "wmup": warmupsec = Integer.parseInt(arg[1]); break;
				default: System.out.printf("Invalid argument %s.\n%s",iArg,usage); return;
			}
		}

		System.out.println(params);

		Record recGen;
		if (rs == 16) {
			recGen =  new SixteenRecord();
		} else if (rs == SampleRecord.length()) {
			recGen = new SampleRecord();
		} else {
			recGen = new BytesRecord(rs);
		}

		assert recGen.size() == rs;

		//assert nf >= tc : "Total number of files must be greater than total number of clients";
		//assert nf % tc == 0 : "nf % tc should be 0, i.e., files should be equally distributed";
		//int nfpc = nf / tc;

		System.out.printf("Warmup sec=%d, test sec=%d, record size=%d\n", warmupsec, td, recGen.size());

		if (mgc) {
			GCMonitor.initialize();
		}

		pr = new Printer();
		pr.print("Starting client " + cid);

		if (type.equals("rw"))
			runRWTest(testID, iat, wr, bs, cid, td, nc, nf, sip, sport, recGen, warmupsec, tc, mid, mip, mport, wtMs, fp);
		else {
			runTest(testID, cid, td, nc, nf, sip, sport, bs, recGen, warmupsec, type, tc, mid, mip, mport, wtMs, fp);
		}

		ClientUtils.writeToFile(params.toString(), fp + "/params.txt");

		pr.print("Finished client " + cid);
	}

	private Result[] runTest(int testID, int cid, int testDurSec, int numCients, int filesPerClient,
								  String svrIP, int sport, int batchSize, Record recGen, int warmupsec, String type,
								  int totalClients, int mid, String mip, int mport, int initWaitMs, String filePrefix) throws InterruptedException, KawkabException {
		TestRunner at = new TestRunner();

		if (cid == mid) {
			at.startServer(totalClients, mport, mid);
		}

		if (initWaitMs > 0) {
			initWait(initWaitMs);
		}

		TestType ttype;
		switch (type) {
			case "apnd":  ttype = TestType.APPEND; break;
			case "noop": ttype = TestType.NOOP; break;
			case "read": ttype = TestType.READ; break;
			default: System.out.println("Invalid test type " + type); return null;
		}


		Result[] results = at.runTest(testID, testDurSec, numCients, cid, filesPerClient, svrIP, sport, batchSize, recGen, pr, warmupsec, ttype,
				totalClients, mid, mip, mport, filePrefix);

		if (cid == mid) {
			at.stopServer();
		}

		return results;
	}

	private void runRWTest(int testID, double intArrTime, int writeRatio, int apBatchSize, int cidOffset, int testDurSec,
						   int clientsPerMachine, int filesPerClient, String svrIP, int sport, Record recGen, int warmupsec,
						   int totalClients, int mid, String mip, int mport, int initWaitMs, String filePrefix)
								throws KawkabException, InterruptedException {
		TestRunnerAsync at = new TestRunnerAsync();

		if (cidOffset == mid) {
			at.startServer(totalClients, mport, mid);
		}

		if (initWaitMs > 0) {
			initWait(initWaitMs);
		}

		at.runTest(testID, intArrTime, writeRatio, testDurSec, totalClients, clientsPerMachine, cidOffset, filesPerClient,
				svrIP, sport, apBatchSize, recGen, warmupsec, mid, mip, mport, filePrefix);

		if (cidOffset == mid) {
			at.stopServer();
		}
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
