package ar.edu.itba.pod.query4;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

public class Query4Combiner implements CombinerFactory<String, Double, Double[]> {

    @Override
    public Combiner<Double, Double[]> newCombiner(String s) {
        return new Combiner<Double, Double[]>() {
            private Double lowestFine = null;

            private Double highestFine = null;

            @Override
            public void reset() {
                lowestFine = 0d;
                highestFine = 0d;
            }

            @Override
            public void combine(Double aDouble) {
                if (lowestFine == null || aDouble < lowestFine) {
                    lowestFine = aDouble;
                }
                if (highestFine == null || aDouble > highestFine) {
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
