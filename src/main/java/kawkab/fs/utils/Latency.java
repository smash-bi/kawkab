package kawkab.fs.utils;

public class Latency {
	public final int min;
	public final int max;
	public final double p25;
	public final double p50;
	public final double p75;
	public final double p95;
	public final double p99;
	public final double mean;

	public Latency(int min, int max, double p25, double p50, double p75, double p95, double p99, double mean){
		this.min = min;
		this.max = max;
		this.p25 = p25;
		this.p50 = p50;
		this.p75 = p75;

		this.p95 = p95;
		this.p99 = p99;
		this.mean = mean;
	}
}
