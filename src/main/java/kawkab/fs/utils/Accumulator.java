package kawkab.fs.utils;

public class Accumulator{
    private long[] buckets;
    //private long totalSum;
    private long totalCnt;
    //private double prodSum = 0;
    //private int maxCntBucket;
    private double maxValue;
    private double minValue = Double.MAX_VALUE;

    //private double aggMean;
    //private double m2;
    
    
    public Accumulator(){
        reset();
    }
    
    public synchronized void reset(){
        int maxValue = 1000000; //in microseconds
        buckets = new long[maxValue];
        //maxCntBucket = 0;
        this.maxValue = 0;
        //totalSum = 0;
        totalCnt = 0;
        //prodSum = 0;
        this.minValue = Double.MAX_VALUE;
    }
    
    public synchronized void put(int value) {
        assert value >= 0 : "Value is negative: " + value;

        int bucket = value;

        if (bucket >= buckets.length){
            bucket = buckets.length-1;
        }
        
        buckets[bucket]++;
        //totalSum += value;
        totalCnt += 1;

        //long prod = value*value;

        //assert prodSum + prod > 0: String.format("prodSum is rolled over: old=%,.2f, new=%d, sum=%,.2f, value=%d", prodSum, prod, (prodSum+prod), value);

        //prodSum += prod;
        
        //if (buckets[maxCntBucket] < buckets[bucket])
        //   maxCntBucket = bucket;

        if (maxValue < value)
            maxValue = value;
        
        if (minValue > value)
            minValue = value;

        // Welford's online algorithm
        //https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Welford's_online_algorithm
        //double delta = value - aggMean;
        //aggMean += delta / totalCnt;
        //double delta2 = value - aggMean;
        //m2 += delta * delta2;
    }
    
    public synchronized double mean(){
        /*if (totalCnt > 0)
            return 1.0*totalSum/totalCnt;
        else
            return -1;*/

        //return aggMean;

        return mean(buckets);
    }
    
    public synchronized double min(){
        return minValue;
    }
    
    public synchronized double max(){
        return maxValue;
    }
    
    /*public double variance(){
        if (totalCnt > 2)
            //return (prodSum - (1.0*totalSum*totalSum/totalCnt))/totalCnt;
            return m2 /totalCnt;
        else
            return -1;

    }
    
    public synchronized double stdDev(){
        if (variance() > 0)
            return Math.sqrt(variance());
        else
            return -1;
    }*/
    
    public synchronized long count() {
        return totalCnt;
    }
    
    /**
     * @return Returns [median lat, 95 %ile lat, 99 %ile lat]
     * */
    public synchronized double[] getLatencies() {
        int medianBucket = buckets.length;
        int perc95Bucket = buckets.length;
        int perc99Bucket = buckets.length;
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
        double[] lats = getLatencies();
        return String.format("50%%=%.2f, 95%%=%.2f, 99%%=%.2f, min=%.2f, max=%.2f, mean=%.2f",
                lats[0],lats[1],lats[2], min(), max(), mean());
    }
    
    public synchronized void printCDF() {
        long[] cdf2 = cdf();
        for(int i=1; i<=100; i++) {
            System.out.print(i+"="+cdf2[i]+", ");
        }
        System.out.println();
    }
    
    synchronized void print(){
        for (int i=0; i<buckets.length; i++){
            if (buckets[i] > 0)
                System.out.print((i+1)+":"+buckets[i]+", ");
        }
        System.out.println();
    }
    
    public synchronized long[] histogram(){
        return buckets;
    }
    
    public synchronized long[] cdf(){
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

        //totalSum += from.totalSum;
        totalCnt += from.totalCnt;
        //prodSum += from.prodSum;
        //aggMean = (aggMean+from.aggMean)/2.0;
        //aggMean = mean(buckets);
        //m2 += this.m2;

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
