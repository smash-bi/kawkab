package kawkab.fs.utils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class HistogramMap {
	private Map<Integer, Count> bucketMap;
	private long totalSum;
	private long totalCnt;
	private double prodSum = 0;
	private int max;
	private int min = Integer.MAX_VALUE;

	public HistogramMap(int initailSize) {
		bucketMap = new HashMap<>(initailSize);
	}

	public synchronized void add(int val) {
		assert val > 0;

		Count c = bucketMap.get(val);
		if (c == null) {
			c = new Count();
			bucketMap.put(val, c);
		}

		c.count++;

		totalSum += val;
		totalCnt += 1;
		prodSum += (val * val);

		if (max < val)
			max = val;

		if (min > val)
			min = val;
	}

	public double getAverage() {
		if (totalCnt > 0)
			return 1.0 * totalSum / totalCnt;
		else
			return -1;
	}

	public int getMin() {
		return min;
	}

	public int getMax() {
		return max;
	}

	double getVariance() {
		if (totalCnt > 0)
			return (prodSum - (1.0*totalSum * totalSum / totalCnt)) / totalCnt;
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
		int medianBucket = -2; // -2 to indicate null value
		int perc95Bucket = -2;
		int perc99Bucket = -2;
		long sum = 0;
		int maxVal = getMax();
		for (int i = getMin(); i <= maxVal; i++) {
			Count c = bucketMap.get(i);
			if (c == null || c.count <= 0) continue;
			assert Long.MAX_VALUE - c.count > sum : "Long value overflow";

			sum += c.count; //WARN: integer overflow



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
	/*public static double[] getCdf(Map<Integer, Count> hist) {
		//int k = hist.length;
		long n = 0;

		int maxVal = 0;
		int minVal = Integer.MIN_VALUE;
		for (Map.Entry<Integer, Count> entry : hist.entrySet()) { // sum all histogram values
			int val = entry.getKey();
			Count c = entry.getValue();
			assert c.count <= Long.MAX_VALUE - n : "Long value overflow"; // Detect overflow
			n += c.count;

			if (minVal > val) minVal = val;
			if (maxVal < val) maxVal = val;
		}

		double[] cdf = new double[k]; // cdf table 'cdf'
		long c = 0; // Cumulative histogram values
		for (int i = minVal; i <= maxVal; i++) {
			Count cnt = hist.get(i);
			if (cnt == null) continue;

			assert Long.MAX_VALUE - c > cnt.count;

			c += cnt.count;
			cdf[i] = (double) c / n;
		}

		return cdf;
	}*/

	void print() {
		int maxVal = getMax();
		for (int i = getMin(); i <= maxVal; i++) {
			Count c = bucketMap.get(i);
			if (c == null || c.count <= 0) continue;

			System.out.println(((i + 1)) + ":" + c.count + ",");
		}
	}

	public Map<Integer, Count> histogram() {
		return Collections.unmodifiableMap(bucketMap);
	}

	public long[] cdf() {
		long[] cdf = new long[101];
		double cnt = 0;
		int maxVal = getMax();
		for (int i = getMin(); i <= maxVal; i++) {
			Count c = bucketMap.get(i);
			if (c == null)
				continue;

			assert Double.MAX_VALUE - c.count > cnt : "Double value overflow";

			cnt += c.count;
			cdf[(int) (Math.ceil(cnt / totalCnt * 100.0))] = i;
		}

		return cdf;
	}

	private class Count{
		private int count;
	}
}
