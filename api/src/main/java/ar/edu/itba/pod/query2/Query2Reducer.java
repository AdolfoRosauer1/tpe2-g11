package ar.edu.itba.pod.query2;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class Query2Reducer implements ReducerFactory<String, Double, Double> {
    @Override
    public Reducer<Double, Double> newReducer(String s) {
        return new Reducer<Double, Double>() {
            private double sum = 0;

            @Override
            public void reduce(Double aDouble) {
                sum += aDouble;
            }

            @Override
            public Double finalizeReduce() {
                return sum;
            }
        };
    }
}
