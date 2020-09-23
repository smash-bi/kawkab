package kawkab.fs.utils;

public class Accumulator {
    private long[] buckets;
    private long totalCnt;
    private int maxValue;
    private int minValue = Integer.MAX_VALUE;

    public Accumulator(int numBuckets) {
        buckets = new long[numBuckets];
        reset();
    }

    public Accumulator(long[] histogram, long totalCnt, int min, int max) {
        this.buckets = histogram;
        this.totalCnt = totalCnt;
        this.minValue = min;
        this.maxValue = max;
    }
    
    public synchronized void reset() {
        for (int i = 0, len = buckets.length; i < len; i++) {
            buckets[i] = 0;
        }
        this.maxValue = 0;
        totalCnt = 0;
        this.minValue = Integer.MAX_VALUE;
    }

    public synchronized void put(int value, int count) {
        assert count >= 0 : "Count is negative: " + count;
        assert value >= 0 : "Bucket number is negative: " + count;

        int bucket = value;

        if (bucket >= buckets.length){
            bucket = buckets.length-1;
        }

        assert Long.MAX_VALUE - totalCnt >= count;

        buckets[bucket] += count;
        totalCnt += count;

        if (maxValue < value)
            maxValue = value;

        if (minValue > value)
            minValue = value;
    }
    
    public synchronized double mean(){
        return mean(buckets);
    }
    
    public synchronized int min(){
        return minValue;
    }
    
    public synchronized int max(){
        return maxValue;
    }
    
    public synchronized long count() {
        return totalCnt;
    }
    
    /**
     * @return Returns [median lat, 95 %ile lat, 99 %ile lat]
     * */
    public synchronized double[] getLatencies() {
        int medianBucket = -1;
        int perc95Bucket = -1;
        int perc99Bucket = -1;
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
            else if (sum == buckets[i])
                medianBucket = perc95Bucket = perc99Bucket = i;
        }
        
        return new double[]{ medianBucket, perc95Bucket, perc99Bucket };
    }
    
    /*Adapted from the book "Digital Image Processing: An Algorithmic Introduction Using Java", 
     * page 70, Figure 5.3: five point operations*/
    private synchronized double[] histToCdf(long[] hist){
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
    public synchronized String toString() {
        if (totalCnt == 0)
            return "No stats";

        double[] lats = getLatencies();
        return String.format("50%%=%.2f, 95%%=%.2f, 99%%=%.2f, min=%d, max=%d, mean=%.2f", lats[0],lats[1],lats[2], min(), max(), mean());
    }
    
    public synchronized void printCDF() {
        long[] cdf2 = cdf();
        for(int i=1; i<=100; i++) {
            System.out.print(i+"="+cdf2[i]+", ");
        }
        System.out.println();
    }
    
    public synchronized void print(){
        for (int i=0; i<buckets.length; i++){
            if (buckets[i] > 0)
                System.out.print((i+1)+",");
        }

        System.out.println();
        for (int i=0; i<buckets.length; i++){
            if (buckets[i] > 0)
                System.out.print(buckets[i]+", ");
        }

        System.out.println();
    }
    
    public synchronized long[] buckets(){
        return buckets;
    }
    
    public synchronized long[] cdf() {
        long[] cdf = new long[101];
        long cnt = 0; 
        for(int i=0; i<buckets.length; i++){
            cnt += buckets[i];
            cdf[(int)(Math.ceil(cnt*1.0/totalCnt*100.0))] = i;
        }
        
        return cdf;
    }

    public synchronized void merge(Accumulator from) {
        assert buckets.length == from.buckets.length;

        for (int i=0; i<buckets.length; i++) {
            buckets[i] += from.buckets[i];
        }

        totalCnt += from.totalCnt;

        if (maxValue < from.maxValue)
            maxValue = from.maxValue;

        if (minValue > from.minValue)
            minValue = from.minValue;
    }

    private double mean(long[] hist) {
        double avg = 0;
        int n = 1;
        for (int x=0; x<hist.length; x++) {
            for (int j=0; j<hist[x]; j++) {
                avg += (x - avg) / n;
                ++n;
            }
        }
        return avg;
    }
}
