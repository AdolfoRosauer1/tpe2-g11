package ar.edu.itba.pod.client.queries;

import ar.edu.itba.pod.client.utils.Utils;
import ar.edu.itba.pod.models.ticket.Ticket;
import ar.edu.itba.pod.query2.Query2Collator;
import ar.edu.itba.pod.query2.Query2Combiner;
import ar.edu.itba.pod.query2.Query2Mapper;
import ar.edu.itba.pod.query2.Query2Reducer;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static ar.edu.itba.pod.client.Client.logger;

public class Query2 implements Query {
    private final HazelcastInstance hazelcastInstance;
    private final Job<Long, Ticket> job;
    private List<String> results;
    private Map<String, String> infractions;
    private List<String> agencies;
    private IMap<Long,Ticket> tickets;

    private String city;

    public Query2(HazelcastInstance hazelcastInstance) {
        logger.info("Creating Query2");

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



        logger.info("Query2 created");
    }


    @Override
    public void loadFromPath(String path, String city) {
        logger.info("query2 loading from " + path, "\t city: " + city);
        try {
            Utils.loadTicketsFromPathAndUpload(path, city, tickets);
            infractions = Utils.loadInfractionsFromPath(path, city);
            agencies = Utils.loadAgenciesFromPath(path, city);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        logger.info("query2 loaded");
    }

    @Override
    public void run() {
        logger.info("query2 running");
        try{
            ICompletableFuture<List<String>> future = job
                    .mapper(new Query2Mapper())
                    .combiner(new Query2Combiner())
                    .reducer(new Query2Reducer())
                    .submit(new Query2Collator(agencies));
            results = future.get();
        } catch (Exception e) {
            e.printStackTrace();
        }
        logger.info("Query2 completed");
    }

    @Override
    public String getResults() {
        if (results == null) {
            return null;
        }
        logger.info("Getting Query2 results");
        StringBuilder sb = new StringBuilder();
        results.forEach(s -> sb.append(s).append("\n"));

        // cleanup
        tickets.clear();
        tickets.destroy();

        return sb.toString();
    }
}
