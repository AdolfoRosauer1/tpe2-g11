package ar.edu.itba.pod.query4;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

public class Query4Combiner implements CombinerFactory<String, Integer, Integer[]> {

    @Override
    public Combiner<Integer, Integer[]> newCombiner(String s) {
        return new Combiner<>() {
            private Integer lowestFine = null;

            private Integer highestFine = null;

            @Override
            public void reset() {
                lowestFine = 0;
                highestFine = 0;
            }

            @Override
            public void combine(Integer integer) {
                if (lowestFine == null || integer < lowestFine) {
                    lowestFine = integer;
                }
                if (highestFine == null || integer > highestFine) {
                    highestFine = integer;
                }
            }

            @Override
            public Integer[] finalizeChunk() {
                return new Integer[]{lowestFine, highestFine};
            }
        };
    }
}
