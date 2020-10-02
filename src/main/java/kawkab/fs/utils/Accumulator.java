package kawkab.fs.utils;

import java.util.Arrays;
import java.util.Collections;

public class Accumulator {
    private int[] buckets;
    private long totalCnt;
    private int maxValue;
    private int minValue = Integer.MAX_VALUE;

    public Accumulator(int numBuckets) {
        buckets = new int[numBuckets];
        reset();
    }

    public Accumulator(int[] buckets) {
        this.buckets = buckets;
        for (int i=0; i<buckets.length; i++) {
            int c = buckets[i];

            assert Long.MAX_VALUE - totalCnt >= c;

            totalCnt += c;
            if (minValue > c) minValue = c;
            if (maxValue < c) maxValue = c;
        }
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
        assert Integer.MAX_VALUE - buckets[bucket] >= count;

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
    public synchronized Latency getLatencies() {
        int p25 = -1;
        int p50 = -1;
        int p95 = -1;
        int p75 = -1;
        int p99 = -1;
        long sum = 0;
        for (int i = 0; i < buckets.length; i++) {
            if (buckets[i] <= 0)
                continue;

            assert Long.MAX_VALUE - sum > buckets[i];

            sum += buckets[i];
            if (sum <= 0.25 * totalCnt)
                p25 = i;
            else if (sum <= 0.5 * totalCnt)
                p50 = i;
            else if (sum <= 0.75 * totalCnt)
                p75 = i;
            else if (sum <= 0.95 * totalCnt)
                p95 = i;
            else if (sum <= 0.99 * totalCnt)
                p99 = i;
            else if (sum == buckets[i])
                p25 = p50 = p75 = p95 = p99 = i;
        }
        
        return new Latency(min(), max(), p25, p50, p75, p95, p99, mean());
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

        Latency lats = getLatencies();
        return String.format("50%%=%.2f, 95%%=%.2f, 99%%=%.2f, min=%d, max=%d, mean=%.2f, 25%%=%.2f, 75%%=%.2f",
                        lats.p50,lats.p95,lats.p99, min(), max(), mean(), lats.p25, lats.p75);
    }
    
    public synchronized void printCDF() {
        long[] cdf2 = cdf();
        for(int i=1; i<=100; i++) {
            System.out.print(i+"="+cdf2[i]+", ");
        }
        System.out.println();
    }
    
    public synchronized void print(){
        System.out.print("[");
        for (int i=0; i<buckets.length; i++){
            if (buckets[i] > 0)
                System.out.print((i+1)+",");
        }

        System.out.print("], [");
        for (int i=0; i<buckets.length; i++){
            if (buckets[i] > 0)
                System.out.print(buckets[i]+", ");
        }

        System.out.println("]");
    }

    public synchronized void printPairs(){
        System.out.print("[");
        for (int i=0; i<buckets.length; i++){
            if (buckets[i] > 0)
                System.out.print((i+1)+":"+buckets[i]+", ");
        }
        System.out.println("]");
    }
    
    public synchronized int[] buckets(){
        return buckets;
    }
    
    public synchronized long[] cdf() {
        long[] cdf = new long[101];
        long cnt = 0; 
        for(int i=0; i<buckets.length; i++){
            assert Long.MAX_VALUE + cnt >= buckets[i];
            cnt += buckets[i];
            cdf[(int)(Math.ceil(cnt*1.0/totalCnt*100.0))] = i;
        }
        
        return cdf;
    }

    public synchronized void merge(Accumulator from) {
        assert buckets.length == from.buckets.length;
        assert Long.MAX_VALUE - totalCnt >= from.totalCnt;

        for (int i=0; i<buckets.length; i++) {
            assert  Integer.MAX_VALUE - buckets[i] >= from.buckets[i];
            buckets[i] += from.buckets[i];
        }

        totalCnt += from.totalCnt;

        if (maxValue < from.maxValue)
            maxValue = from.maxValue;

        if (minValue > from.minValue)
            minValue = from.minValue;
    }

    private double mean(int[] hist) {
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

    public double countsMean() {
        double avg = 0;
        int n = 1;
        for (int i=0; i<buckets.length; i++) {
            int bc = buckets[i];
            avg += (bc - avg) / n;
            ++n;
        }

        //System.out.printf("Mean=%.2f, len=%d, : %s\n", avg, buckets.length, Arrays.toString(buckets));
        return avg;
    }

    public Accumulator copyOf() {
        Accumulator accm = new Accumulator(buckets.length);
        for (int i=0; i<buckets.length; i++) {
            accm.buckets[i] = buckets[i];
        }

        accm.totalCnt = totalCnt;
        accm.maxValue = maxValue;
        accm.minValue = minValue;

        return accm;
    }

    public int[] values() {
        int[] vals = new int[buckets.length];
        for (int i=0; i<vals.length; i++) {
            vals[i] = buckets[i];
        }
        return vals;
    }

    public int numBuckets() {
        return buckets.length;
    }

    public void sortedBucketPairs(StringBuilder indexVals, StringBuilder counts){
        int i;
        for (i = 0; i < buckets.length-1; i++) {
            indexVals.append(i+", ");
            counts.append(buckets[i]+", ");
        }

        indexVals.append(i);
        counts.append(buckets[i]);
    }
}
