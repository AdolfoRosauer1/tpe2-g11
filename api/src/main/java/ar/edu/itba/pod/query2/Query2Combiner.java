package ar.edu.itba.pod.query2;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

public class Query2Combiner implements CombinerFactory<String, Double, Double> {

    @Override
    public Combiner<Double, Double> newCombiner(String s) {
        return new Combiner<Double,Double>() {
            private double sum = 0;

            @Override
            public void reset() {
                sum = 0;
            }

            @Override
            public void combine(Double aDouble) {
                sum += aDouble;
            }

            @Override
            public Double finalizeChunk() {
                return sum;
            }
        };
    }
}
