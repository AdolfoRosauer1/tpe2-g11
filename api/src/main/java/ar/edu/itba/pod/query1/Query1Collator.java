package ar.edu.itba.pod.query1;

import com.hazelcast.mapreduce.Collator;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class Query1Collator implements Collator<Map.Entry<String, Integer>, List<String>> {
    private final Map<String, String> infractions;

    public Query1Collator(Map<String, String> infractions) {
        this.infractions = infractions;
    }
    @Override
    public List<String> collate(Iterable<Map.Entry<String, Integer>> iterable) {
//        TODO: replace infraction code with infraction name (key to value in the infraction map)
        return (StreamSupport.stream(iterable.spliterator(), false)
                .sorted((e1, e2) -> {
                    int compare = e2.getValue().compareTo(e1.getValue());
                    if (compare == 0) {
                        String[] parts1 = e1.getKey().split(";");
                      String[] parts2 = e2.getKey().split(";");
                      int infrCompare = parts1[0].compareTo(parts2[0]);
                      if (infrCompare != 0) return infrCompare;
                        return parts1[1].compareTo(parts2[1]);
                    }
                    return compare;
                })
                .filter(e -> e.getValue() > 0)
                .map(e -> {
                    String[] parts = e.getKey().split(";");
                    String infractionName = infractions.get(parts[0]);
                    return infractionName + ";" + parts[1] + ";" + e.getValue();
                })
                .collect(Collectors.toList()));
    }
}
