package kawkab.fs.testclient;

import kawkab.fs.utils.Accumulator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class ClientUtils {
	public static Result prepareResult(Accumulator accm, int testDurSec, int recSize, int nc, int nf, int batchSize, boolean print) {
		long cnt = accm.count()*batchSize;
		double sizeMB = recSize*cnt / (1024.0 * 1024.0);

		if (print) {
			System.out.println(String.format("Result: dur=%d sec, batch=%d, clnts=%d, files=%d, size=%.2f MB, thr=%,.2f MB/s, opThr=%,.0f, Lat(us): %s\n",
					testDurSec, batchSize, nc, nf * nc, sizeMB, accm.dataTput(), accm.opsTput(), accm));
		}

		double[] lats = accm.getLatencies();
		return new Result(cnt, accm.opsTput(), accm.dataTput(), lats[0], lats[1], lats[2], accm.min(), accm.max(), accm.mean(),
				0, lats[0], 0, accm.mean(), 0, accm.max(), new long[]{}, accm.histogram());
	}

	public static void writeToFile(String lines, String outFile) {
		File file = new File(outFile).getParentFile();
		if (!file.exists()) {
			file.mkdirs();
		}

		try (BufferedWriter writer = new BufferedWriter(new FileWriter(outFile));) {
			writer.write(lines);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static Accumulator merge(Accumulator[] accms) {
		Accumulator accm = new Accumulator();

		for (int i=0; i<accms.length; i++) {
			accm.merge(accms[i]);
		}

		return accm;
	}

	public static void saveResult(Result res, String filePrefix) {
		res.exportJson(filePrefix+".json", false);
		res.exportJson(filePrefix+"-hists.json", true);
		res.exportCsv(filePrefix+".csv");
	}

}
