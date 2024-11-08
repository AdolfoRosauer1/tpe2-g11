package ar.edu.itba.pod.query3;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class Query3Reducer implements ReducerFactory<String, int[], Double> {
    @Override
    public Reducer<int[], Double> newReducer(String region) {
        return new Reducer<>() {
            private int reincidentSum = 0;
            private int totalSum = 0;

            @Override
            public void reduce(int[] counts) {
                reincidentSum += counts[0];
                totalSum += counts[1];
            }

            @Override
            public Double finalizeReduce() {
                if (totalSum == 0) return 0.0;
                return (double) reincidentSum / totalSum;
            }
        };
    }
}
