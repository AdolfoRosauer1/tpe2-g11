package ar.edu.itba.pod.query3;

import ar.edu.itba.pod.models.ticket.Ticket;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.time.LocalDate;

public class Query3Mapper implements Mapper<Long, Ticket, String, Integer> {

    private final LocalDate from;
    private final LocalDate to;

    public Query3Mapper() {
        this.from = null;
        this.to = null;
    }

    public Query3Mapper(LocalDate from, LocalDate to) {
        this.from = from;
        this.to = to;
    }

    public Query3Mapper(LocalDate from, boolean isFrom) {
        if (isFrom) {
            this.from = from;
            this.to = null;
        } else {
            this.from = null;
            this.to = from;
        }
    }

    @Override
    public void map(Long aLong, Ticket ticket, Context<String, Integer> context) {
        if (from != null && ticket.getIssueDate().isBefore(from)) {
            return;
        }
        if (to != null && ticket.getIssueDate().isAfter(to)) {
            return;
        }
        String key = ticket.getRegion() + ";" + ticket.getLicensePlate() + ";" + ticket.getInfractionCode();
        context.emit(key, 1);
    }
}
