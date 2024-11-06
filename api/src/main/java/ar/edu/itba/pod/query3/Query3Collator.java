package ar.edu.itba.pod.query3;

import com.hazelcast.mapreduce.Collator;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class Query3Collator implements Collator<Map.Entry<String, Double>, List<String>> {

    @Override
    public List<String> collate(Iterable<Map.Entry<String, Double>> iterable) {
        return (StreamSupport.stream(iterable.spliterator(), false)
                .sorted((e1, e2) -> {
                    int compare = e2.getValue().compareTo(e1.getValue());

                    if (compare == 0){
                        String region1 = e1.getKey().split(";")[0];
                        String region2 = e2.getKey().split(";")[0];

                        return region1.compareTo(region2);
                    }

                    return compare;
                })
                .map(e -> e.getKey() + ";" + String.format("%.2f", (e.getValue() * 100)) + "%")
                .collect(Collectors.toList()));
    }
}
