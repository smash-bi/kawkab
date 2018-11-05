package kawkab.fs.commons;

public class Stats {
    private long count;
    private double sum;
    private double prodSum;
    private double min;
    private double max;
//    private double throughput;
    
    public Stats(){
        min = Double.MAX_VALUE;
        max = 0;
    }
    
    public void clear(){
        sum = prodSum = min = max = 0;
        count = 0;
    }
    
    public synchronized void putValue(double value){
        count += 1;
        sum += value;
        prodSum += value*value;
        
        if (min > value) min = value;
        if (max < value) max = value;
    }
    
    public double mean(){
        return sum/count;
    }
    
    public double variance(){
        return (prodSum - (sum*sum/count))/count;
    }
    
    public double stddev(){
        return Math.sqrt(variance());
    }
    
    public double min(){
        return min;
    }
    
    public double max(){
        return max;
    }
    
    public long count(){
        return count;
    }
    
    public double sum(){
        return sum;
    }
    
    @Override
    public String toString(){
        return String.format("a=%d s=%d n=%d x=%d, N=%d", (long)mean(), (long)stddev(), (long)min, (long)max, count);
    }
}
