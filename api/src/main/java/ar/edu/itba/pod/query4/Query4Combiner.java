package ar.edu.itba.pod.query4;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

public class Query4Combiner implements CombinerFactory<String, Double, Double[]> {

    @Override
    public Combiner<Double, Double[]> newCombiner(String s) {
        return new Combiner<Double, Double[]>() {
            private Double lowestFine = 1000.0;

            private Double highestFine = 0.0;

            @Override
            public void reset() {
                lowestFine = 1000.0;
                highestFine = 0.0;
            }

            @Override
            public void combine(Double aDouble) {
                if (lowestFine == 1000.0 || aDouble < lowestFine) {
                    lowestFine = aDouble;
                }
                if (highestFine == 0.0 || aDouble > highestFine) {
                    highestFine = aDouble;
                }
            }

            @Override
            public Double[] finalizeChunk() {
                return new Double[]{lowestFine, highestFine};
            }
        };
    }
}
