package kawkab.fs.utils;

public class Histogram {
	private long[] buckets;
	private long totalSum;
	private long totalCnt;
	private double prodSum = 0;
	private int maxCntBucket;
	private double maxLatency;
	private double minLatency = Double.MAX_VALUE;

	public Histogram(int numBuckets) {
		buckets = new long[numBuckets];
	}

	public synchronized void add(int latUsec) {
		assert latUsec >= 1;

		int bucket = latUsec;
		if (bucket >= buckets.length) {
			bucket = buckets.length - 1;
		} else if (bucket < 0) {
			assert bucket >= 0;
			return;
		}

		buckets[bucket]++;
		totalSum += latUsec;
		totalCnt += 1;
		prodSum += (latUsec * latUsec);

		if (buckets[maxCntBucket] < buckets[bucket])
			maxCntBucket = bucket;

		if (maxLatency < latUsec)
			maxLatency = latUsec;

		if (minLatency > latUsec)
			minLatency = latUsec;
	}

	public double getAverage() {
		if (totalCnt > 0)
			return 1.0 * totalSum / totalCnt;
		else
			return -1;
	}

	public double getMinLatency() {
		return minLatency;
	}

	public double getMaxLatency() {
		return maxLatency;
	}

	double getVariance() {
		if (totalCnt > 0)
			return (prodSum - (totalSum * totalSum / totalCnt)) / totalCnt;
		else
			return -1;
	}

	double getStdDev() {
		if (getVariance() > 0)
			return Math.sqrt(getVariance());
		else
			return -1;
	}

	/**
	 * @return Returns [median lat, 95 %ile lat, 99 %ile lat]
	 */
	public double[] getLatencies() {
		int medianBucket = buckets.length;
		int perc95Bucket = buckets.length;
		int perc99Bucket = buckets.length;
		long sum = 0;
		for (int i = 0; i < buckets.length; i++) {
			if (buckets[i] <= 0)
				continue;
			sum += buckets[i];
			if (sum <= 0.5 * totalCnt)
				medianBucket = i;
			else if (sum <= 0.95 * totalCnt)
				perc95Bucket = i;
			else if (sum <= 0.99 * totalCnt)
				perc99Bucket = i;
		}

		return new double[] { medianBucket, perc95Bucket, perc99Bucket };
	}

	/*
	 * Adapted from the book
	 * "Digital Image Processing: An Algorithmic Introduction Using Java", page 70,
	 * Figure 5.3: five point operations
	 */
	public static double[] getCdf(long[] hist) {
		int k = hist.length;
		long n = 0;
		for (int i = 0; i < k; i++) { // sum all histogram values
			assert n + hist[i] <= Long.MAX_VALUE;
			n += hist[i];
		}

		double[] cdf = new double[k]; // cdf table 'cdf'
		long c = 0; // Cumulative histogram values
		for (int i = 0; i < k; i++) {
			c += hist[i];
			cdf[i] = (double) c / n;
		}

		return cdf;
	}

	void print() {
		for (int i = 0; i < buckets.length; i++) {
			if (buckets[i] == 0)
				continue;
			System.out.println(((i + 1)) + ":" + buckets[i] + ",");
		}
	}

	public long[] histogram() {
		return buckets;
	}

	public long[] cdf() {
		long[] cdf = new long[101];
		long cnt = 0;
		for (int i = 0; i < buckets.length; i++) {
			cnt += buckets[i];
			cdf[(int) (Math.ceil(cnt * 1.0 / totalCnt * 100.0))] = i;
		}

		return cdf;
	}
}
