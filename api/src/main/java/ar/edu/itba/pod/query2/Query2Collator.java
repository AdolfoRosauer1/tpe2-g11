package ar.edu.itba.pod.query2;

import com.hazelcast.mapreduce.Collator;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class Query2Collator implements Collator<Map.Entry<String, Double>, List<String>> {

    private final List<String> agencies;

    public Query2Collator(List<String> agencies) {
        this.agencies = agencies;
    }

    @Override
    public List<String> collate(Iterable<Map.Entry<String, Double>> iterable) {

        final AtomicReference<Double> lastMonthYTD = new AtomicReference<>(0d);
        final AtomicInteger lastYearRegistered = new AtomicInteger(0);

        return StreamSupport.stream(iterable.spliterator(), false)
                .sorted((e1, e2) -> {
                    String[] parts1 = e1.getKey().split(";");
                    String[] parts2 = e2.getKey().split(";");

                    int compare = parts1[0].compareTo(parts2[0]);
                    if (compare == 0) {
                        int compare2 = parts1[1].compareTo(parts2[1]);
                        if (compare2 == 0) {
                            return Integer.compare(Integer.parseInt(parts1[2]), Integer.parseInt(parts2[2]));
                        }
                        return compare2;
                    }
                    return compare;
                })
                .filter(e -> e.getValue() > 0)
                .map(e -> {
                    String[] parts = e.getKey().split(";");
                    int currentYear = Integer.parseInt(parts[1]);
                    double value = e.getValue();

                    // Calculate YTD based on whether it's the same year as the previous entry
                    double YTD = lastYearRegistered.get() == currentYear ? lastMonthYTD.get() + value : value;

                    // Update tracking variables
                    lastMonthYTD.set(YTD);
                    lastYearRegistered.set(currentYear);

                    return e.getKey() + ";" + String.format("%.0f", YTD);
                })
                .filter(e -> agencies == null || agencies.contains(e.split(";")[0]))
                .collect(Collectors.toList());
    }
}
