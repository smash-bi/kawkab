package kawkab.fs.utils;

public class Accumulator{
    private long[] buckets;
    public long totalSum;
    public long totalCnt;
    double prodSum = 0;
    private int maxCntBucket;
    public double maxValue;
    public double minValue = Double.MAX_VALUE;
    
    
    public Accumulator(){
        reset();
    }
    
    synchronized void reset(){
        int maxValue = 2000000; //in microseconds
        buckets = new long[maxValue];
        maxCntBucket = 0;
        this.maxValue = 0;
        totalSum = 0;
        totalCnt = 0;
        prodSum = 0;
        this.maxValue = 0;
        this.minValue = Double.MAX_VALUE;
    }
    
    public synchronized void put(int value){
        int bucket = value;
        if (bucket >= buckets.length){
            bucket = buckets.length-1;
        } else if (bucket < 0){
            assert bucket >= 0;
            return;
        }
        
        buckets[bucket]++;
        totalSum += value;
        totalCnt += 1;
        prodSum += (value*value);
        
        if (buckets[maxCntBucket] < buckets[bucket])
            maxCntBucket = bucket;

        if (maxValue < value)
            maxValue = value;
        
        if (minValue > value)
            minValue = value;
    }
    
    public double mean(){
        if (totalCnt > 0)
            return 1.0*totalSum/totalCnt;
        else
            return -1;
    }
    
    public double min(){
        return minValue;
    }
    
    public double max(){
        return maxValue;
    }
    
    double variance(){
        if (totalCnt > 0)
            return (prodSum - (totalSum*totalSum/totalCnt))/totalCnt;
        else
            return -1;
    }
    
    double stdDev(){
        if (variance() > 0)
            return Math.sqrt(variance());
        else
            return -1;
    }
    
    /**
     * @return Returns [median lat, 95 %ile lat, 99 %ile lat]
     * */
    public double[] getLatencies() {
        int medianBucket = buckets.length;
        int perc95Bucket = buckets.length;
        int perc99Bucket = buckets.length;
        long sum = 0;
        for (int i = 0; i < buckets.length; i++) {
            /*if (buckets[i].count <= 0)
                continue;
            sum += buckets[i].count;*/
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
        
        return new double[]{ medianBucket, perc95Bucket, perc99Bucket };
    }
    
    /*Adapted from the book "Digital Image Processing: An Algorithmic Introduction Using Java", 
     * page 70, Figure 5.3: five point operations*/
    private double[] histToCdf(long[] hist){
       int k = hist.length;
       int n = 0;
       for(int i=0; i<k; i++) { //sum all histogram values
           n += hist[i];
       }
       
       double[] cdf = new double[k]; //cdf table 'cdf'
       long c = hist[0]; //cumulative histogram values
       cdf[0] = (double) c / n;
       for(int i=1; i<k; i++){
           c += hist[i];
           cdf[i] = (double) c / n;
       }
       
       return cdf;
    }
    
    @Override
    public String toString() {
        double[] lats = getLatencies();
        return String.format("50%%=%.2f, 95%%=%.2f, 99%%=%.2f, min=%.2f, max=%.2f,mean=%.2f,stdev=%.2f",
                lats[0],lats[1],lats[2], min(), max(), mean(), stdDev());
    }
    
    public void printCDF() {
        long[] cdf2 = cdf();
        for(int i=1; i<=100; i++) {
            System.out.print(i+"="+cdf2[i]+", ");
        }
        System.out.println();
    }
    
    void print(){
        for (int i=0; i<buckets.length; i++){
            if (buckets[i] > 0)
                System.out.print((i+1)+":"+buckets[i]+", ");
        }
        System.out.println();
    }
    
    public long[] histogram(){
        return buckets;
    }
    
    public long[] cdf(){
        long[] cdf = new long[101];
        long cnt = 0; 
        for(int i=0; i<buckets.length; i++){
            cnt += buckets[i];
            cdf[(int)(Math.ceil(cnt*1.0/totalCnt*100.0))] = i;
        }
        
        return cdf;
    }
}
