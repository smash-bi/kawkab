package kawkab.fs.utils;

import com.google.common.base.Stopwatch;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * TimeLog measures latency of an operation p.
 * Usage semantics: timeLog.start(); p(); timeLog.end();
 *
 * TimeLog takes measurements based on the given sample rate. If the rate is 100%, all the start/end pairs are included
 * in the measurements. Otherwise, the start/end pair is included by following a uniform distribution.
 *
 * This class is not thread safe.
 */
public class TimeLog {
	private TimeUnit unit;
	private Accumulator stats;
	private boolean started;
	private long tCount;
	private String tag;
	private int[] rand;
	private int randIdx;
	private int samplePercent;
	private Stopwatch sw;

	public TimeLog(TimeUnit unit, String tag, int samplePercent) {
		assert 0 <= samplePercent && samplePercent <= 100 : "Sample percent must be between 0 and 100";
		this.unit = unit;
		this.tag = tag;
		this.samplePercent = samplePercent;
		stats = new Accumulator();

		int numRands = 100000;
		rand = new int[numRands];
		Random r = new Random();
		for(int i=0; i<numRands; i++) {
			rand[i] = r.nextInt(100);
		}

		started = false;
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
		
		sw = Stopwatch.createStarted();
	}
	
	public void end() {
		if (!started)
			return;

		int diff = (int)(sw.stop().elapsed(unit)); //Diff can be negative due to using nanoTime() in multi-cpu hardware
		if (diff >= 0)
			stats.put(diff);

		started = false;
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
		stats = new Accumulator();
		tCount = 0;
	}

	public Accumulator accumulator() {
		return stats;
	}
}
