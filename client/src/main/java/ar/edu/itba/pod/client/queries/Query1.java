package ar.edu.itba.pod.client.queries;

import ar.edu.itba.pod.client.utils.Utils;
import ar.edu.itba.pod.models.ticket.Ticket;
import ar.edu.itba.pod.query1.Query1Collator;
import ar.edu.itba.pod.query1.Query1Combiner;
import ar.edu.itba.pod.query1.Query1Mapper;
import ar.edu.itba.pod.query1.Query1Reducer;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static ar.edu.itba.pod.client.Client.logger;

public class Query1 implements Query {
    private final Job<Long, Ticket> job;
    private List<String> results;
    private Map<String, String> infractions;
    private IMap<Long,Ticket> tickets;

    public Query1(HazelcastInstance hazelcastInstance) {
        logger.info("Creating Query1");

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

        logger.info("Query1 created");
    }


    @Override
    public void loadFromPath(String path, String city) {
        logger.info("Query1 loading from " + path, "\t city: " + city);
        try {
            Utils.loadTicketsFromPathAndUpload(path, city, tickets);
            infractions = Utils.loadInfractionsFromPath(path, city);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        logger.info("Query1 loaded");
    }

    @Override
    public void run() {
        logger.info("Query1 running");
        try{
            ICompletableFuture<List<String>> future = job
                    .mapper(new Query1Mapper())
                    .combiner(new Query1Combiner())
                    .reducer(new Query1Reducer())
                    .submit(new Query1Collator(infractions));
            results = future.get();
        } catch (Exception e) {
            e.printStackTrace();
        }
        logger.info("Query1 completed");
    }

    @Override
    public String getResults() {
        if (results == null) {
            return null;
        }
        logger.info("Getting Query1 results");
        StringBuilder sb = new StringBuilder();
        results.forEach(s -> sb.append(s).append("\n"));

        // cleanup
        tickets.clear();
        tickets.destroy();

        return sb.toString();
    }
}
