package ar.edu.itba.pod.client.queries;

import ar.edu.itba.pod.models.ticket.Ticket;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static ar.edu.itba.pod.client.Client.logger;

public class Query4 implements Query {
    private final HazelcastInstance hazelcastInstance;
    private final Job<Long, Ticket> job;
    private List<String> results;
    private Map<String, String> infractions;
    private List<String> agencies;
    private IMap<Long,Ticket> tickets;
    private String city;

    public Query4(HazelcastInstance hazelcastInstance) {
        logger.info("Creating Query4");

        this.hazelcastInstance = hazelcastInstance;
        tickets = hazelcastInstance.getMap("tickets");
        logger.info("Tickets IMap created");
        tickets.clear();
        logger.info("Tickets IMap cleared");

        JobTracker jobTracker = hazelcastInstance.getJobTracker(String.valueOf(Instant.now().getEpochSecond()));
        logger.info("JobTracker created");
        KeyValueSource<Long, Ticket> source = KeyValueSource.fromMap(tickets);
        logger.info("Ticket source created");
        job = jobTracker.newJob(source);
        logger.info("Job created");

        logger.info("Query4 created");
    }
    @Override
    public void loadFromPath(String path, String city) {

    }

    @Override
    public void run() {

    }

    @Override
    public String getResults() {
        return "";
    }
}
