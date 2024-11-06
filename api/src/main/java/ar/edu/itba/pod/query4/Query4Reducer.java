package ar.edu.itba.pod.query4;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class Query4Reducer implements ReducerFactory<String, Double[], Double[]> {
    @Override
    public Reducer<Double[], Double[]> newReducer(String s) {
        return new Reducer<Double[], Double[]>() {

            private Double lowestFine = null;

            private Double highestFine = null;


            @Override
            public void reduce(Double[] doubles) {
                if (lowestFine == null || doubles[0] < lowestFine) {
                    lowestFine = doubles[0];
                }
                if (highestFine == null || doubles[1] > highestFine) {
                    highestFine = doubles[1];
                }
            }

            @Override
            public Double[] finalizeReduce() {
                return new Double[]{lowestFine, highestFine};
            }

        };
    }
}
