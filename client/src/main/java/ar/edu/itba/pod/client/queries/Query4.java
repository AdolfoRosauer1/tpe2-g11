package ar.edu.itba.pod.client.queries;

import ar.edu.itba.pod.client.utils.Utils;
import ar.edu.itba.pod.models.ticket.Ticket;
import ar.edu.itba.pod.query3.Query3Collator;
import ar.edu.itba.pod.query3.Query3Combiner;
import ar.edu.itba.pod.query3.Query3Mapper;
import ar.edu.itba.pod.query3.Query3Reducer;
import ar.edu.itba.pod.query4.Query4Collator;
import ar.edu.itba.pod.query4.Query4Combiner;
import ar.edu.itba.pod.query4.Query4Mapper;
import ar.edu.itba.pod.query4.Query4Reducer;
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

public class Query4 implements Query {
    private final Job<Long, Ticket> job;
    private List<String> results;
    private Map<String, String> infractions;
    private IMap<Long,Ticket> tickets;
    private final String agency;
    private final int n;

    public Query4(HazelcastInstance hazelcastInstance, String agency, int n) {
        logger.info("Creating Query4");

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

        this.agency = agency;
        this.n = n;

        logger.info("Query4 created");
    }
    @Override
    public void loadFromPath(String path, String city) {
        try{
            Utils.loadTicketsFromPathAndUpload(path, city, tickets);
            infractions = Utils.loadInfractionsFromPath(path, city);
        }catch(Exception e){
            e.printStackTrace();
        }
        logger.info("Query4 loaded");
    }

    @Override
    public void run() {
        logger.info("Query4 running");
        try{
            ICompletableFuture<List<String>> future = job
                    .mapper(new Query4Mapper(agency))
                    .combiner(new Query4Combiner())
                    .reducer(new Query4Reducer())
                    .submit(new Query4Collator(n,infractions));
            results = future.get();
        } catch (Exception e) {
            e.printStackTrace();
        }
        logger.info("Query4 completed");
    }

    @Override
    public String getResults() {
        if (results == null) {
            return null;
        }
        logger.info("Getting Query4 results");
        StringBuilder sb = new StringBuilder();
        results.forEach(s -> sb.append(s).append("\n"));

        // cleanup
        tickets.clear();
        tickets.destroy();

        return sb.toString();
    }
}
