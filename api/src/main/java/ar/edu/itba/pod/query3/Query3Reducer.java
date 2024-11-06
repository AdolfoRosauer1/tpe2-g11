package ar.edu.itba.pod.query3;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class Query3Reducer implements ReducerFactory<String, Boolean, Double> {
    @Override
    public Reducer<Boolean, Double> newReducer(String s) {
        return new Reducer<>() {
            private double reincidentSum = 0;
            private double nonReincidentSum = 0;

            @Override
            public void reduce(Boolean aBoolean) {
                if (aBoolean) {
                    reincidentSum++;
                } else {
                    nonReincidentSum++;
                }
            }

            @Override
            public Double finalizeReduce() {
                return reincidentSum / (reincidentSum + nonReincidentSum);
            }
        };
    }
}
