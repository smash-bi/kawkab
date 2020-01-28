package kawkab.fs.testclient;

import kawkab.fs.utils.Accumulator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;

public class Result {
	private long reqsCount; //Total requests
	private double opsThr; //ops per second
	private double dataThr; // Data throughput
	private double latMin;
	private double latMax;
	private long[] latHist;
	private long[] tputLog;
	private int recsTput;

	public Result() {
		latMin = Double.MAX_VALUE;
	}

	public Result(long reqsCount_, double opsThr_, double dataThr_,
				  double lMin, double lMax, long[] latHist_, long[] tputLog_, int recsTput_) {
		reqsCount = reqsCount_; opsThr = opsThr_; dataThr = dataThr_;
		latMin = lMin; latMax=lMax;
		latHist = latHist_;
		tputLog = tputLog_;
		recsTput = recsTput_;
	}

	public Result(Result res) {
		reqsCount = res.reqsCount; opsThr = res.opsThr; dataThr = res.dataThr;
		latMin = res.latMin; latMax=res.latMax;
		latHist = Arrays.copyOf(res.latHist, res.latHist.length);
		tputLog = Arrays.copyOf(res.tputLog, res.tputLog.length);
		recsTput = res.recsTput;
	}

	/*public Result(Accumulator latHistAccm, long[] tputLog) {
		reqsCount = latHistAccm.count();
		opsThr = latHistAccm.opsTput();
		dataThr = latHistAccm.dataTput();
		latMin = latHistAccm.min(); latMax = latHistAccm.max();
		latHist = latHistAccm.histogram();
		this.tputLog = tputLog;
	}*/

	public long count() {
		return reqsCount;
	}

	public long[] latHist() {
		return latHist;
	}

	public double opsTput() {
		return opsThr;
	}

	public double dataTput() {
		return dataThr;
	}

	public double latMin() {
		return latMin;
	}

	public double latMax() {
		return latMax;
	}

	public long[] tputLog() {
		return tputLog;
	}

	public int recsTput() { return recsTput; }

	public Accumulator latAccumulator() {
		return new Accumulator(latHist, reqsCount, latMin, latMax);
	}

	public void merge(Result from) {
		if (latHist == null || latHist.length == 0)
			latHist = new long[from.latHist.length];

		if (tputLog == null || tputLog.length == 0)
			tputLog = new long[from.tputLog.length];

		assert latHist.length == from.latHist.length;
		assert tputLog.length == from.tputLog.length;

		reqsCount += from.reqsCount;
		opsThr += from.opsThr;
		dataThr += from.dataThr;
		recsTput += from.recsTput;

		if (latMin > from.latMin)
			latMin = from.latMin;

		if (latMax < from.latMax)
			latMax = from.latMax;

		for(int i=0; i<latHist.length; i++) {
			latHist[i] += from.latHist[i];
		}

		for(int i=0; i<tputLog.length; i++) {
			tputLog[i] += from.tputLog[i];
		}
	}

	public String toJson(boolean exportHists, boolean exportTputLog){
		return toJsonSB(exportHists, exportTputLog).toString();
	}

	private StringBuilder toJsonSB(boolean exportHists, boolean exportTputLog){
		Accumulator accm = new Accumulator(latHist, reqsCount, latMin, latMax);
		double[] lats = accm.getLatencies();
		StringBuilder json = new StringBuilder();
		json.append("{ ");
		json.append(String.format("  \"reqs\":%,d, ", reqsCount));
		json.append(String.format("  \"opsPs\":%,.0f, ", opsThr));
		json.append(String.format("  \"thrMBps\":%.2f, ", dataThr));
		json.append(String.format("  \"meanLat\":%.2f, ", accm.mean()));
		json.append(String.format("  \"50%%Lat\":%.2f, ", lats[0]));
		json.append(String.format("  \"95%%Lat\":%.2f, ", lats[1]));
		json.append(String.format("  \"99%%Lat\":%.2f, ", lats[2]));
		json.append(String.format("  \"minLat\":%.2f, ", latMin));
		json.append(String.format("  \"maxLat\":%.2f, ", latMax));
		json.append(String.format("  \"recsPs\":%d, ", recsTput));

		if (exportHists) {
			appendHists(json);
		}

		if (exportTputLog && tputLog != null) {
			appendTputLog(json);
		}

		json.append("}");

		return json;
	}

	private void appendTputLog(StringBuilder sb) {
		StringBuilder index = new StringBuilder();
		StringBuilder counts = new StringBuilder();

		int lastIdx;
		for(lastIdx=tputLog.length-1; lastIdx >= 0; lastIdx--) {
			if (tputLog[lastIdx] > 0)
				break;
		}

		int i;
		for (i=0; i<lastIdx; i++) {
			index.append(i+", ");
			counts.append(tputLog[i]+", ");
		}

		index.append(i);
		counts.append(tputLog[i]);

		sb.append(",  \"TputLog\":{");
		sb.append("\"TimeSec\":[").append(index.toString()).append("]");
		sb.append(", \"Counts\":[").append(counts.toString()).append("]");
		sb.append("}\n");
	}

	private void appendHists(StringBuilder sb) {
		if (latHist == null || latHist.length == 0)
			return;

		StringBuilder lats = new StringBuilder();
		StringBuilder counts = new StringBuilder();
		int i=0;
		for (i=0; i<latHist.length-1; i++){
			if (latHist[i] > 0) {
				lats.append(i+",");
				counts.append(latHist[i]+",");
			}
		}

		lats.append(i);
		counts.append(latHist[i]);

		sb.append(",  \"Latency Histogram\":{");
		sb.append("\"count\":[").append(counts.toString()).append("]");
		sb.append(", \"latency\":[").append(lats.toString()).append("]");
		sb.append("}\n");
	}

	public String csvHeader() {
		StringBuilder header = new StringBuilder("#");
		header.append("#");
		header.append("\"Requests successful\", ");
		header.append("\"Ops throughput (Ops/s)\", ");
		header.append("\"Throughput (MiB/s)\", ");
		header.append("\"Mean latency\", ");
		header.append("\"Median latency\", ");
		header.append("\"95%% latency\", ");
		header.append("\"99%% latency\", ");
		header.append("\"Min latency\", ");
		header.append("\"Max latency\", ");
		header.append("\"Records throughput\"");

		return header.toString();
	}


	public String csv() {
		Accumulator accm = new Accumulator(latHist, reqsCount, latMin, latMax);
		double[] lats = accm.getLatencies();

		StringBuilder csv = new StringBuilder();
		csv.append(String.format("%d, ", reqsCount));
		csv.append(String.format("%.0f, ", opsThr));
		csv.append(String.format("%.2f, ", dataThr));
		csv.append(String.format("%.0f, ", accm.mean()));
		csv.append(String.format("%.0f, ", lats[0]));
		csv.append(String.format("%.0f, ", lats[1]));
		csv.append(String.format("%.0f, ", lats[2]));
		csv.append(String.format("%.0f, ", latMin));
		csv.append(String.format("%.0f, ", latMax));
		csv.append(String.format("%d\n", recsTput));

		return csv.toString();
	}

	public synchronized void exportCsv(String outFile) {
		StringBuilder csv = new StringBuilder();
		csv.append(csvHeader());
		csv.append("\n");
		csv.append(csv());
		writeToFile(csv.toString(), outFile);
	}

	public synchronized void exportJson(String outFile, boolean exportHists, boolean exportTputLog) {
		StringBuilder json = new StringBuilder();
		json.append("[\n");
		json.append(toJsonSB(exportHists, exportTputLog));
		json.append("\n]");

		writeToFile(json.toString(), outFile);
	}

	private synchronized void writeToFile(String lines, String outFile) {
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
}
