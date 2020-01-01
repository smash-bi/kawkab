package kawkab.fs.testclient;

public class SyncResponse {
	public final long count;
	public final double opsTput;
	public final double tput;
	public final double lat50;
	public final double lat95;
	public final double lat99;
	public final double latMin;
	public final double latMax;
	public final double latMean;
	public final boolean stopAll;


	public SyncResponse(long count, double opsTput, double tput, double lat50, double lat95, double lat99, double latMin, double latMax, double latMean, boolean stopAll) {
		this.count = count;
		this.opsTput = opsTput;
		this.tput = tput;
		this.lat50 = lat50;
		this.lat95 = lat95;
		this.lat99 = lat99;
		this.latMin = latMin;
		this.latMax = latMax;
		this.latMean = latMean;
		this.stopAll = stopAll;
	}
}
