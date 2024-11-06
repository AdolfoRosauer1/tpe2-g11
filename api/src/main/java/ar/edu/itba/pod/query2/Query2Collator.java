package ar.edu.itba.pod.query2;

import com.hazelcast.mapreduce.Collator;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class Query2Collator implements Collator<Map.Entry<String, Integer>, List<String>> {

    @Override
    public List<String> collate(Iterable<Map.Entry<String, Integer>> iterable) {
        return (StreamSupport.stream(iterable.spliterator(), false)
                .sorted((e1, e2) -> {
                    String[] parts1 = e1.getKey().split(";");
                    String[] parts2 = e2.getKey().split(";");

                    int compare = parts1[0].compareTo(parts2[0]);

                    if (compare == 0) {
                        int compare2 = parts1[1].compareTo(parts2[1]);
                        if (compare2 == 0) {
                            return parts1[2].compareTo(parts2[2]);
                        }
                        return compare2;
                    }

                    return compare;
                })
                .filter(e -> e.getValue() > 0)
                .map(e -> e.getKey() + ";" + e.getValue())
                .collect(Collectors.toList()));
    }
}
