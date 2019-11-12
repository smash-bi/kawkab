package kawkab.fs.testclient;

public class Result {
	long reqsCount; //Total requests
	int opsThr; //ops per second
	double lat50;
	double lat95;
	double lat99;
	double latMin;
	double latMax;
	double latAvg;
	double dataThr; // Data throughput
	double readMedLat;
	double writeMedLat;
	double readAvgLat;
	double writeAvgLat;
	double readMaxLat;
	double writeMaxLat;
	long[] readHistogram;
	long[] writeHistogram;

	Result(long reqsCount_, int opsThr_, double myThr_,
		   double l50, double l95, double l99, double lMin, double lMax, double lAvg,
		   double readMedLat_, double writeMedLat_,
		   double readAvgLat_, double writeAvgLat_,
		   double readMaxLat_, double writeMaxLat_,
		   long[] readHistogram_, long[] writeHistogram_){
		reqsCount=reqsCount_; opsThr =opsThr_; dataThr =myThr_;
		lat50=l50; lat95=l95; lat99=l99; latMin=lMin; latMax=lMax; latAvg=lAvg;
		readMedLat=readMedLat_; writeMedLat=writeMedLat_;
		readAvgLat=readAvgLat_; writeAvgLat=writeAvgLat_;
		readMaxLat=readMaxLat_; writeMaxLat=writeMaxLat_;
		readHistogram=readHistogram_; writeHistogram=writeHistogram_;
	}

	Result(Result res) {
		reqsCount=res.reqsCount; opsThr =res.opsThr; dataThr =res.dataThr;
		lat50=res.lat50; lat95=res.lat95; lat99=res.lat99; latMin=res.latMin; latMax=res.latMax; latAvg=res.latAvg;
		readMedLat=res.readMedLat; writeMedLat=res.writeMedLat;
		readAvgLat=res.readAvgLat; writeAvgLat=res.writeAvgLat;
		readMaxLat=res.readMaxLat; writeMaxLat=res.writeMaxLat;
		readHistogram=res.readHistogram; writeHistogram=res.writeHistogram;
	}

	public String toJson(boolean exportHists){
		StringBuilder json = new StringBuilder();
		json.append(String.format("  \"Requests successful\":%d,\n", reqsCount));
		json.append(String.format("  \"Ops throughput (OPS)\":%d,\n", opsThr));
		json.append(String.format("  \"Throughput (MiB/s)\":%.2f,\n", dataThr));
		json.append(String.format("  \"Mean latency\":%.2f,\n", latAvg));
		json.append(String.format("  \"Median latency\":%.2f,\n", lat50));
		json.append(String.format("  \"95%% latency\":%.2f,\n", lat95));
		json.append(String.format("  \"99%% latency\":%.2f,\n", lat99));
		json.append(String.format("  \"Min latency\":%.2f,\n", latMin));
		json.append(String.format("  \"Max latency\":%.2f,\n", latMax));
		json.append(String.format("  \"Median read latency\":%.2f,\n", readMedLat));
		json.append(String.format("  \"Median write latency\":%.2f,\n", writeMedLat));
		json.append(String.format("  \"Max read latency\":%.2f,\n", readMaxLat));
		json.append(String.format("  \"Max write latency\":%.2f,\n", writeMaxLat));
		json.append(String.format("  \"Average read latency\":%.2f,\n", readAvgLat));
		json.append(String.format("  \"Average write latency\":%.2f\n", writeAvgLat));

		if (exportHists) {
			long[][] hists = {readHistogram,     writeHistogram};
			String[] types = {"Reads histogram", "Writes histogram"};

			for (int k=0; k<hists.length; k++){
				long[] hist = hists[k];
				String type = types[k];

				if (hist != null) {
					StringBuilder lats = new StringBuilder();
					StringBuilder counts = new StringBuilder();
					int i=0;
					for (i=0; i<hist.length-1; i++){
						if (hist[i] > 0) {
							lats.append(i+",");
							counts.append(hist[i]+",");
						}
					}

					lats.append(i);
					counts.append(hist[i]);

					json.append(",  \""+type+"\":{");
					json.append("\"count\":["+counts.toString()+"]");
					json.append(", \"latency\":["+lats.toString()+"]");
					json.append("}\n");
				}
			}

		}

		return json.toString();
	}

	public String toCSV() {
		StringBuilder csv = new StringBuilder();
		csv.append(String.format("\"Requests successful\", ", reqsCount));
		csv.append(String.format("\"Ops throughput (OPS)\", ", opsThr));
		csv.append(String.format("\"Throughput (MiB/s)\", ", dataThr));
		csv.append(String.format("\"Mean latency\":%.2f,\n", latAvg));
		csv.append(String.format("\"Median latency\":%.2f,\n", lat50));
		csv.append(String.format("\"95%% latency\":%.2f,\n", lat95));
		csv.append(String.format("\"99%% latency\":%.2f,\n", lat99));
		csv.append(String.format("\"Min latency\":%.2f,\n", latMin));
		csv.append(String.format("\"Max latency\":%.2f,\n", latMax));
		csv.append(String.format("\"Median read latency\":%.2f,\n", readMedLat));
		csv.append(String.format("\"Median write latency\":%.2f,\n", writeMedLat));
		csv.append(String.format("\"Max read latency\":%.2f,\n", readMaxLat));
		csv.append(String.format("\"Max write latency\":%.2f,\n", writeMaxLat));
		csv.append(String.format("\"Average read latency\":%.2f,\n", readAvgLat));
		csv.append(String.format("\"Average write latency\":%.2f\n", writeAvgLat));

		return csv.toString();
	}
}
