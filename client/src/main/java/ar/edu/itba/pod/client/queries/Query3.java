package ar.edu.itba.pod.client.queries;

import ar.edu.itba.pod.client.utils.Utils;
import ar.edu.itba.pod.models.ticket.Ticket;
import ar.edu.itba.pod.query1.Query1Collator;
import ar.edu.itba.pod.query1.Query1Combiner;
import ar.edu.itba.pod.query1.Query1Mapper;
import ar.edu.itba.pod.query1.Query1Reducer;
import ar.edu.itba.pod.query3.Query3Collator;
import ar.edu.itba.pod.query3.Query3Combiner;
import ar.edu.itba.pod.query3.Query3Mapper;
import ar.edu.itba.pod.query3.Query3Reducer;
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

public class Query3 implements Query {
    private final HazelcastInstance hazelcastInstance;
    private final Job<Long, Ticket> job;
    private List<String> results;
    private Map<String, String> infractions;
    private List<String> agencies;
    private IMap<Long,Ticket> tickets;
    private String city;

    public Query3(final HazelcastInstance hazelcastInstance) {
        logger.info("Creating Query3");

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

        logger.info("Query3 created");
    }
    @Override
    public void loadFromPath(String path, String city) {
        try{
            Utils.loadTicketsFromPathAndUpload(path, city, tickets);
            infractions = Utils.loadInfractionsFromPath(path, city);
            agencies = Utils.loadAgenciesFromPath(path, city);
        }catch(Exception e){
            e.printStackTrace();
        }
        logger.info("Query3 loaded");
    }

    @Override
    public void run() {
        logger.info("Query3 running");
        try{
            ICompletableFuture<List<String>> future = job
                    .mapper(new Query3Mapper())
                    .combiner(new Query3Combiner())
                    .reducer(new Query3Reducer())
                    .submit(new Query3Collator());
            results = future.get();
        } catch (Exception e) {
            e.printStackTrace();
        }
        logger.info("Query3 completed");
    }

    @Override
    public String getResults() {
        return "";
    }
}
