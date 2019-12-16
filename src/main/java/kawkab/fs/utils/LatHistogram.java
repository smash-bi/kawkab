package kawkab.fs.utils;

import com.google.common.base.Stopwatch;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * LatHistogram measures latency of an operation p.
 * Usage semantics: timeLog.start(); p(); timeLog.end();
 *
 * LatHistogram takes measurements based on the given sample rate. If the rate is 100%, all the start/end pairs are included
 * in the measurements. Otherwise, the start/end pair is included by following a uniform distribution.
 *
 * This class is not thread safe.
 */
public class LatHistogram {
	private TimeUnit unit;
	private Accumulator stats;
	private boolean started;
	private long tCount;
	private String tag;
	private int[] rand;
	private int randIdx;
	private int samplePercent;
	private Stopwatch sw;
	private int batchSize;

	public LatHistogram(TimeUnit unit, String tag, int samplePercent, int numBuckets) {
		this(unit, tag, samplePercent, numBuckets, 1);
	}

	public LatHistogram(TimeUnit unit, String tag, int samplePercent, int numBuckets, int batchSize) {
		assert 0 <= samplePercent && samplePercent <= 100 : "Sample percent must be between 0 and 100";
		this.unit = unit;
		this.tag = tag;
		this.samplePercent = samplePercent;
		stats = new Accumulator(numBuckets);
		this.batchSize = batchSize;

		int numRands = 100000;
		rand = new int[numRands];
		Random r = new Random();
		for(int i=0; i<numRands; i++) {
			rand[i] = r.nextInt(100);
		}

		started = false;

		sw = Stopwatch.createUnstarted();
	}
	
	
	/**
	 * start() and end() must be called in a combination, surrounding the code that is being timed
	 */
	public void start() {
		// Allow repeated starts, the last one wins
		tCount++;
		randIdx = (randIdx+1) % rand.length;
		if (rand[randIdx] >= samplePercent)
			return;
		
		started = true;

		sw.reset();
		sw.start();
	}
	
	public int end() {
		if (!started)
			return -1;

		int lastDur = (int)(sw.stop().elapsed(unit)); //Diff can be negative due to using nanoTime() in multi-cpu hardware
		if (lastDur >= 0)
			stats.put(lastDur, batchSize);

		started = false;

		return lastDur;
	}

	
	public void printStats() {
		System.out.printf("\t%s\n", getStats());
	}
	
	public String getStats() {
		if (stats.count() == 0)
			return tag + ": No stats";

		return String.format("%s (%s): %s, count=%,d, sampled=%,d", tag, abbv(unit), stats, tCount,stats.count());
	}

	private String abbv(TimeUnit unit) {
		switch(unit) {
			case NANOSECONDS:
				return "ns";
			case MICROSECONDS:
				return "us";
			case MILLISECONDS:
				return "ms";
			case SECONDS:
				return "sec";
			default:
				return unit.toString();
		}
	}

	public long count() {
		return tCount;
	}

	public long sampled() {
		return stats.count();
	}

	public double[] stats() {
		return stats.getLatencies();
	}

	public double min() {
		return stats.min();
	}

	public double max() {
		return stats.max();
	}

	public double mean() {
		return stats.mean();
	}
	
	public void reset() {
		stats.reset();
		tCount = 0;
	}

	public Accumulator accumulator() {
		return stats;
	}
}
