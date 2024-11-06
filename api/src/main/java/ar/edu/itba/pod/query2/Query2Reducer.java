package ar.edu.itba.pod.query2;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class Query2Reducer implements ReducerFactory<String, Integer, Integer> {
    @Override
    public Reducer<Integer, Integer> newReducer(String s) {
        return new Reducer<>() {
            private int sum = 0;


            @Override
            public void reduce(Integer integer) {
                sum += integer;
            }

            @Override
            public Integer finalizeReduce() {
                return sum;
            }
        };
    }
}
