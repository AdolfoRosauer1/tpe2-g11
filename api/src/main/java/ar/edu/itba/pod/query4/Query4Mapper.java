package ar.edu.itba.pod.query4;

import ar.edu.itba.pod.models.ticket.Ticket;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class Query4Mapper implements Mapper<Long, Ticket, String, Double> {

    private final String agency;

    Query4Mapper() {
        this.agency = null;
    }

    Query4Mapper(String agency) {
        this.agency = agency;
    }

    @Override
    public void map(Long aLong, Ticket ticket, Context<String, Double> context) {
        if (agency != null && !ticket.agency().equals(agency)) {
            return;
        }
        // We use a composite key in case a same infraction can be emitted by different agencies
        String key = ticket.infractionCode() + ";" + ticket.agency();
        context.emit(key, ticket.fineAmount());
    }
}
