package ar.edu.itba.pod.query3;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

public class Query3Combiner implements CombinerFactory<String, Integer, Boolean> {

    private final int minimumTickets;

    public Query3Combiner(int minimumTickets) {
        this.minimumTickets = minimumTickets;
    }

    public Query3Combiner() {
        this.minimumTickets = 2;
    }

    @Override
    public Combiner<Integer, Boolean> newCombiner(String s) {
        return new Combiner<>() {
            private double sum = 0;

            @Override
            public void reset() {
                sum = 0;
            }


            @Override
            public void combine(Integer integer) {
                sum += integer;
            }

            @Override
            public Boolean finalizeChunk() {
                return sum >= minimumTickets;
            }
        };
    }
}
