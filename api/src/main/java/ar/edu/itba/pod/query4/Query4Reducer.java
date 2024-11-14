package ar.edu.itba.pod.query4;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class Query4Reducer implements ReducerFactory<String, Double[], Double[]> {
    @Override
    public Reducer<Double[], Double[]> newReducer(String s) {
        return new Reducer<Double[], Double[]>() {

            private Double lowestFine = 1000.0;

            private Double highestFine = 0.0;


            @Override
            public void reduce(Double[] doubles) {
                if (lowestFine == 1000.0 || doubles[0] < lowestFine) {
                    lowestFine = doubles[0];
                }
                if (highestFine == 0.0 || doubles[1] > highestFine) {
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
