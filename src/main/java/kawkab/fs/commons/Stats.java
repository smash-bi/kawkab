package kawkab.fs.commons;

public class Stats {
    private long count;
    private double sum;
    private double prodSum;
    private double min;
    private double max;
    private long toIgnore;
//    private double throughput;
    
    public Stats(){
        min = Double.MAX_VALUE;
        max = 0;
    }
    
    /**
     * @param valsToIgnore Ignore initial valsToIgnore values 
     */
    public Stats(long valsToIgnore){
    	this();
        toIgnore = valsToIgnore;
    }
    
    public synchronized void clear(){
        sum = prodSum = min = max = 0;
        count = 0;
    }
    
    public synchronized void putValue(double value){
    	if (toIgnore > 0) {
    		toIgnore--;
    		return;
    	}
    	
        count += 1;
        sum += value;
        prodSum += value*value;
        
        if (min > value) min = value;
        if (max < value) max = value;
    }
    
    public synchronized double mean(){
        return sum/count;
    }
    
    public synchronized double variance(){
        return (prodSum - (sum*sum/count))/count;
    }
    
    public synchronized double stddev(){
        return Math.sqrt(variance());
    }
    
    public synchronized double min(){
        return min;
    }
    
    public synchronized double max(){
        return max;
    }
    
    public synchronized long count(){
        return count;
    }
    
    public synchronized double sum(){
        return sum;
    }
    
    @Override
    public synchronized String toString(){
        return String.format("a=%,d s=%,d n=%,d x=%,d, N=%,d", (long)mean(), (long)stddev(), (long)min, (long)max, count);
    }
}
