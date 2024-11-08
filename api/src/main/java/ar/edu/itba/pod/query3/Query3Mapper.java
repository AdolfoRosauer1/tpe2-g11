package ar.edu.itba.pod.query3;

import ar.edu.itba.pod.models.ticket.Ticket;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;

public class Query3Mapper implements Mapper<Long, Ticket, String, Boolean> {

    private final LocalDate from;
    private final LocalDate to;
    private final int n;
    private final Map<String, Integer> infractionCount = new HashMap<>();

    public Query3Mapper(LocalDate from, LocalDate to, int n) {
        this.from = from;
        this.to = to;
        this.n = n;
    }

    @Override
    public void map(Long aLong, Ticket ticket, Context<String, Boolean> context) {
        if ((from != null && ticket.issueDate().isBefore(from)) ||
                (to != null && ticket.issueDate().isAfter(to))) {
            return;
        }

        String region = ticket.region();
        String uniqueCombination = region + ";" + ticket.licensePlate() + ";" + ticket.infractionCode();

        infractionCount.put(uniqueCombination, infractionCount.getOrDefault(uniqueCombination, 0) + 1);

        boolean isReincident = infractionCount.get(uniqueCombination) >= n;
        context.emit(region, isReincident);
    }
}
