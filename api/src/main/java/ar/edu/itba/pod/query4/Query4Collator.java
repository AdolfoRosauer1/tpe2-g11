package ar.edu.itba.pod.query4;

import com.hazelcast.mapreduce.Collator;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class Query4Collator implements Collator<Map.Entry<String, Double[]>, List<String>> {

    private final int n;

    public Query4Collator(int n) {
        this.n = n;
    }

    public Query4Collator() {
        this.n = 1;
    }
    @Override
    public List<String> collate(Iterable<Map.Entry<String, Double[]>> iterable) {
        return (StreamSupport.stream(iterable.spliterator(), false)
                .sorted((e1, e2) -> {
                    double diff1 = Math.abs(e1.getValue()[1] - e1.getValue()[0]);
                    double diff2 = Math.abs(e2.getValue()[1] - e2.getValue()[0]);

                    int compare = Double.compare(diff2, diff1);

                    if (compare == 0){
                        String infraction1 = e1.getKey().split(";")[0];
                        String infraction2 = e2.getKey().split(";")[0];

                        return infraction1.compareTo(infraction2);
                    }

                    return compare;

                })
                .limit(n)
                .map(e -> e.getKey().split(";")[0] + ";" + e.getValue()[0] + ";" + e.getValue()[1] + ";" + Math.abs(e.getValue()[1] - e.getValue()[0]))
                .collect(Collectors.toList()));
    }
}
