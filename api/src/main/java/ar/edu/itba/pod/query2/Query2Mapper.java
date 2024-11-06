package ar.edu.itba.pod.query2;

import ar.edu.itba.pod.models.ticket.Ticket;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class Query2Mapper implements Mapper<Long, Ticket, String, Double> {

    @Override
    public void map(Long aLong, Ticket ticket, Context<String, Double> context) {
        String key = ticket.getAgency() + ";" + ticket.getIssueDate().getYear() + ";" + ticket.getIssueDate().getMonth();
        context.emit(key, ticket.getFineAmount());
    }
}
