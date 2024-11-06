package ar.edu.itba.pod.query4;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class Query4Reducer implements ReducerFactory<String, Integer[], Integer[]> {
    @Override
    public Reducer<Integer[], Integer[]> newReducer(String s) {
        return new Reducer<Integer[], Integer[]>() {

            private Integer lowestFine = null;

            private Integer highestFine = null;

            @Override
            public void reduce(Integer[] integers) {
                if (lowestFine == null || integers[0] < lowestFine) {
                    lowestFine = integers[0];
                }
                if (highestFine == null || integers[1] > highestFine) {
                    highestFine = integers[1];
                }
            }

            @Override
            public Integer[] finalizeReduce() {
                return new Integer[]{lowestFine, highestFine};
            }

        };
    }
}
