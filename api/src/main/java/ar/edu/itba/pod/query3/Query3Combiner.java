package ar.edu.itba.pod.query3;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

public class Query3Combiner implements CombinerFactory<String, Boolean, int[]> {
    @Override
    public Combiner<Boolean, int[]> newCombiner(String key) {
        return new Combiner<>() {
            private int reincidentCount = 0;
            private int totalCount = 0;

            @Override
            public void combine(Boolean isReincident) {
                if (isReincident) {
                    reincidentCount++;
                }
                totalCount++;
            }

            @Override
            public int[] finalizeChunk() {
                return new int[] { reincidentCount, totalCount };
            }
        };
    }
}
