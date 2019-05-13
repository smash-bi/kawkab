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
	public TimeLog(TimeLogUnit unit) {
		this.unit = unit;
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
	
	public void printStats(String tag) {
		//System.out.printf("\t%s (%s): %s, num calls: %,d\n", tag, unit.getUnit(), stats, count);
		System.out.printf("\t%s (%s): %s, num calls: %,d\n", tag, unit.getUnit(), stats, count);
	}
	
	public String getStats() {
		return String.format("(%s): %s, num calls: %,d\n", unit.getUnit(), stats, count);
	}
	
	private long timeNow() {
		if (unit == TimeLogUnit.MILLIS)
			return System.currentTimeMillis();
		else
			return System.nanoTime();
	}
}
