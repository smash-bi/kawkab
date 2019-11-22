package kawkab.fs.utils;

import java.time.Clock;
import java.util.Random;

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
	public enum TimeLogUnit {
		MILLIS("ms"), NANOS("ns");
		
		private String unit;
		TimeLogUnit(String unit) {
			this.unit = unit;
		}
		
		public String getUnit() {
			return unit;
		}
		
	};
	private TimeLogUnit unit;
	private long lastTS;
	private Accumulator stats;
	private boolean started;
	private long count;
	private String tag;
	private int[] rand;
	private int randIdx;
	private int samplePercent;

	public TimeLog(TimeLogUnit unit, String tag, int samplePercent) {
		assert 0 <= samplePercent && samplePercent <= 100 : "Sample percent must be between 0 and 100";
		this.unit = unit;
		this.tag = tag;
		this.samplePercent = samplePercent;
		stats = new Accumulator(); //Ignore initial 1000 readings

		int count = 100000;
		rand = new int[count];
		Random r = new Random();
		for(int i=0; i<count; i++) {
			rand[i] = r.nextInt(100);
		}

		started = false;
	}
	
	
	/**
	 * start() and end() must be called in a combination, surrounding the code that is being timed
	 */
	public void start() {
		//assert !started; Allow repeated starts, the last one wins
		count++;
		randIdx = ++randIdx % rand.length;
		if (rand[randIdx] >= samplePercent)
			return;
		
		started = true;
		
		lastTS = timeNow();
	}
	
	public void end() {
		if (!started)
			return;

		int diff = (int)(timeNow() - lastTS); //Diff can be negative due to using nanoTime() in multi-cpu hardware
		if (diff >= 0)
			stats.put(diff);
		
		started = false;
	}
	
	public void printStats() {
		//System.out.printf("\t%s (%s): %s, num calls: %,d\n", tag, unit.getUnit(), stats, count);
		System.out.printf("\t%s\n", getStats());
	}
	
	public String getStats() {
		return String.format("(%s): %s, count=%,d, sampled=%d", unit.getUnit(), stats, count,stats.count());
	}
	
	private long timeNow() {
		if (unit == TimeLogUnit.MILLIS)
			return System.currentTimeMillis();
		else
			return System.nanoTime();
	}

	public long count() {
		return count;
	}

	public long sampled() {
		return stats.count();
	}
	
	public void reset() {
		stats = new Accumulator(); //Ignore initial 1000 readings
		count = 0;
		lastTS = 0;
		stats.reset();
	}
}
