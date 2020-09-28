package kawkab.fs.utils;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class AccumulatorMap {
    private Map<Integer, Count> buckets;
    private long totalCnt;
    private int maxValue;
    private int minValue = Integer.MAX_VALUE;

    public AccumulatorMap() {
        this(1000);
    }

    public AccumulatorMap(int initialSize) {
        buckets = new HashMap<>(initialSize);
        reset();
    }

    public AccumulatorMap(long[] counts) {
        int[] indexVal = new int[counts.length];
        int[] cnts = new int[counts.length];
        for (int i=0; i<indexVal.length; i++) {
            indexVal[i] = i;
            assert counts[i] <= Long.MAX_VALUE;
            cnts[i] = (int)counts[i];
        }

        from(indexVal, cnts);
    }

    public AccumulatorMap(int[] indexVals, int[] counts) {
        from(indexVals, counts);
    }

    private void from (int[] indexVals, int[] counts) {
        assert indexVals.length == counts.length;
        int min = Integer.MAX_VALUE;
        int max = 0;
        long total = 0;

        buckets = new HashMap<>();
        for (int i=0; i<indexVals.length; i++) {
            int k = indexVals[i];
            int v = counts[i];
            buckets.put(k, new Count(v));

            if (min > k) min = k;
            if (max < k) max = k;

            assert Long.MAX_VALUE - total > v;
            total += v;
        }

        totalCnt = total;
        maxValue = max;
        minValue = min;
    }

    public AccumulatorMap(int[] indexVals, int[] counts, long totalCnt, int min, int max) {
        //this.buckets = histMap;
        assert indexVals.length == counts.length;

        buckets = new HashMap<>();
        for (int i=0; i<indexVals.length; i++) {
            buckets.put(indexVals[i], new Count(counts[i]));
        }

        this.totalCnt = totalCnt;
        this.minValue = min;
        this.maxValue = max;
    }
    
    public synchronized void reset() {
        for (Count c : buckets.values())
            c.count = 0;

        totalCnt = 0;
        maxValue = -1;
        minValue = Integer.MAX_VALUE;
    }

    public synchronized void put(int value, int count) {
        assert count >= 0 : "Count is negative: " + count;
        assert value >= 0 : "Bucket number is negative: " + count;
        assert Long.MAX_VALUE - totalCnt >= count : "Total count overflow" ;

        Count c = buckets.get(value);
        if (c == null) {
            c = new Count();
            buckets.put(value, c);
        }

        c.count += count;
        totalCnt += count;

        if (maxValue < value)
            maxValue = value;

        if (minValue > value)
            minValue = value;
    }

    public synchronized double weightedMean() {
        double avg = 0;
        int n = 1;

        int[] keys = sortedKeys();
        for (int ik = 0; ik < keys.length; ik++) {
            int x = keys[ik];
            int cnt = buckets.get(x).count;

            for (long j=0; j<cnt; j++) {
                double toAdd = (x - avg) / n;
                assert Double.MAX_VALUE - toAdd > avg;
                avg += toAdd;
                ++n;
            }
        }
        return avg;
    }

    public synchronized double countsMean() {
        double avg = 0;
        int n = 1;

        int maxKey = maxKey();
        for (int i = 0; i < maxKey; i++) {
            Count c = buckets.get(i);
            if (c == null || c.count == 0) {
                ++n;
                continue;
            }

            int cnt = c.count;
            double toAdd = (cnt - avg) / n;
            assert Double.MAX_VALUE - toAdd > avg;
            avg += toAdd;
            ++n;
        }
        return avg;
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
        int p50 = -1; // -1 indicates null value
        int p75 = -1;
        int p95 = -1;
        int p99 = -1;
        long sum = 0;

        int[] keys = sortedKeys();
        for (int k = 0; k < keys.length; k++) {
            int val = keys[k];
            Count c = buckets.get(val);
            assert c != null;

            if (c.count <= 0)
                continue;
            assert Long.MAX_VALUE - sum > c.count;

            sum += c.count;
            if (sum <= 0.25 * totalCnt)
                p25 = val;
            else if (sum <= 0.5 * totalCnt)
                p50 = val;
            else if (sum <= 0.75 * totalCnt)
                p75 = val;
            else if (sum <= 0.95 * totalCnt)
                p95 = val;
            else if (sum <= 0.99 * totalCnt)
                p99 = val;
            else if (sum == c.count)
                p25 = p75 = p50 = p95 = p99 = val;
        }
        
        //return new double[]{ medianBucket, perc95Bucket, perc99Bucket };
        return new Latency(minValue, maxValue, p25, p50, p75, p95, p99, weightedMean());
    }
    
    @Override
    public synchronized String toString() {
        if (totalCnt == 0)
            return "No stats";

        Latency lats = getLatencies();
        return String.format("50%%=%.2f, 95%%=%.2f, 99%%=%.2f, min=%d, max=%d, mean=%.2f, 25%%=%.2f, 75%%=%.2f",
                lats.p50,lats.p95,lats.p99, lats.min, lats.max, lats.mean, lats.p25, lats.p75);
    }
    
    public synchronized void printCDF() {
        long[] cdf2 = cdf();
        for(int i=1; i<=100; i++) {
            System.out.print(i+"="+cdf2[i]+", ");
        }
        System.out.println();
    }
    
    public synchronized void print(){
        int[] keys = sortedKeys();

        for (int i=0; i<keys.length; i++){
            Count c = buckets.get(keys[i]);

            if (c.count > 0)
                System.out.print((keys[i])+",");
        }

        System.out.println();
        for (int i=0; i<keys.length; i++){
            Count c = buckets.get(keys[i]);
            if (c.count > 0)
                System.out.print(c.count+", ");
        }

        System.out.println();
    }
    public synchronized void printPairs(){
        int[] keys = sortedKeys();

        for (int i=0; i<keys.length; i++){
            Count c = buckets.get(keys[i]);

            if (c.count > 0)
                System.out.printf("%d:%d, ",keys[i], c.count);
        }

        System.out.println();
    }
    
    public synchronized Map<Integer, Count> buckets(){
        return Collections.unmodifiableMap(buckets);
    }

    public synchronized long[] cdf() {
        int[] keys = sortedKeys();
        long[] cdf = new long[101];
        long cnt = 0;

        for(int i=0; i<keys.length; i++){
            Count c = buckets.get(keys[i]);
            assert Long.MAX_VALUE - cnt > c.count;

            cnt += c.count;
            cdf[(int)(Math.ceil((cnt*1.0/totalCnt)*100.0))] = keys[i];
        }

        return cdf;
    }

    public synchronized void merge(AccumulatorMap from) {
        long prevCount = totalCnt;
        assert Long.MAX_VALUE - totalCnt > from.totalCnt;
        totalCnt += from.totalCnt;

        if (maxValue < from.maxValue)
            maxValue = from.maxValue;

        if (minValue > from.minValue)
            minValue = from.minValue;

        long newCount = 0;
        for (Map.Entry<Integer, Count> kv : from.buckets.entrySet()){
            int k = kv.getKey();
            Count fromCount = kv.getValue();

            Count c = buckets.get(k);
            if (c == null) {
                c = new Count();
                buckets.put(k, c);
            }

            assert Integer.MAX_VALUE - c.count > fromCount.count;
            c.count += fromCount.count;
            newCount += fromCount.count;
        }

        assert totalCnt > 0;
        assert totalCnt == prevCount + newCount :
                String.format("Counts mismatch: prevTotal=%d, rcvdTotal=%d, newTotal=%d, sum=%d, added=%d addCorrect=%s",
                        prevCount, from.totalCnt, totalCnt, prevCount+newCount, newCount, newCount == from.totalCnt);
    }

    public int[] sortedKeys() {
        int keys[] = new int[buckets.size()];
        int j=0;
        for(int key : buckets.keySet()){
            keys[j++] = key;
        }
        Arrays.sort(keys);

        return keys;
    }

    public int maxKey() {
        int mx = -1;
        for(int key : buckets.keySet()){
            if (mx < key) mx = key;
        }
        return mx;
    }

    /*private synchronized int[] unsortedBucketVals() {
        int[] counts = new int[buckets.size()];
        int i = 0;
        for (Count c : buckets.values()) {
            counts[i++] = c.count;
        }

        return counts;
    }*/

    public synchronized int[] sortedBucketVals() {
        int[] keys = sortedKeys();
        int[] counts = new int[buckets.size()];

        for (int i=0; i<keys.length; i++) {
            counts[i] = buckets.get(keys[i]).count;
        }

        return counts;
    }

    private class Count {
        private int count;
        private Count(){}
        private Count(int c) { count = c; }
    }

    public AccumulatorMap copyOf(){
        AccumulatorMap map = new AccumulatorMap(buckets.size());

        for (Map.Entry<Integer, Count> kv : buckets.entrySet()) {
            int k = kv.getKey();
            Count cnt = new Count(kv.getValue().count);
            map.buckets.put(k, cnt);
        }

        return map;
    }

    public void sortedBucketPairs(StringBuilder indexVals, StringBuilder counts){
        int[] keys = sortedKeys();
        int i;
        for (i = 0; i < keys.length-1; i++) {
            int k = keys[i];
            indexVals.append(k+", ");
            counts.append(buckets.get(k).count+", ");
        }

        indexVals.append(keys[i]);
        counts.append(buckets.get(keys[i]).count);
    }
}
