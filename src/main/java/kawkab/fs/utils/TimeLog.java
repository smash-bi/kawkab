package kawkab.fs.utils;

import java.util.Random;

/*
This class is not thread safe.
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
	private Random rand;
	
	private TimeLogUnit unit;
	private long lastTS;
	private Accumulator stats;
	private boolean started;
	private int count;
	private String tag;
	
	public TimeLog(TimeLogUnit unit, String tag) {
		this.unit = unit;
		this.tag = tag;
		stats = new Accumulator(); //Ignore initial 1000 readings
		rand = new Random();
	}
	
	
	/**
	 * start() and end() must be called in a combination, surrounding the code that is being timed
	 */
	public void start() {
		assert started == false;
		
		count++;
		
		if (rand.nextInt(100) > 3)
			return;
		
		started = true;
		
		lastTS = timeNow();
	}
	
	public void end() {
		if (!started)
			return;
		
		stats.put((int)(timeNow() - lastTS));
		
		started = false;
	}
	
	public void printStats() {
		//System.out.printf("\t%s (%s): %s, num calls: %,d\n", tag, unit.getUnit(), stats, count);
		System.out.printf("\t%s (%s): %s, num calls = %,d, sampled = %d\n", tag, unit.getUnit(), stats, count, stats.count());
	}
	
	public String getStats() {
		return String.format("(%s): %s, total calls = %,d, sampled = %d\n", unit.getUnit(), stats, count,stats.count());
	}
	
	private long timeNow() {
		if (unit == TimeLogUnit.MILLIS)
			return System.currentTimeMillis();
		else
			return System.nanoTime();
	}
	
	public void reset() {
		stats = new Accumulator(); //Ignore initial 1000 readings
		count = 0;
		lastTS = 0;
		stats.reset();
	}
}
