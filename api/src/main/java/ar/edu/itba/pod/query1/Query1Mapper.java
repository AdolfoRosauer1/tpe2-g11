package ar.edu.itba.pod.query1;

import ar.edu.itba.pod.models.ticket.Ticket;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class Query1Mapper implements Mapper<Long, Ticket, String, Integer> {

    @Override
    public void map(Long aLong, Ticket ticket, Context<String, Integer> context) {
        String key = ticket.getInfractionCode() + ";" + ticket.getAgency();
        context.emit(key, 1);
    }
}
