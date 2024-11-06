package ar.edu.itba.pod.client.queries;

import ar.edu.itba.pod.client.utils.Utils;
import ar.edu.itba.pod.models.ticket.Ticket;
import ar.edu.itba.pod.query1.Query1Collator;
import ar.edu.itba.pod.query1.Query1Combiner;
import ar.edu.itba.pod.query1.Query1Mapper;
import ar.edu.itba.pod.query1.Query1Reducer;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;

public class Query1 implements Query {
    private final HazelcastInstance hazelcastInstance;
    private final Job<Long, Ticket> job;
    private List<String> results;
    private Map<Integer, String> infractions;
    private List<String> agencies;
    private IList<Ticket> tickets;

    private String city;

    public Query1(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
        JobTracker jobTracker = hazelcastInstance.getJobTracker(String.valueOf(Instant.now().getEpochSecond()));
        KeyValueSource<Long, Ticket> source = KeyValueSource.fromMap(hazelcastInstance.getMap(String.valueOf(Instant.now().getEpochSecond())));
        job = jobTracker.newJob(source);
    }


    @Override
    public void loadFromPath(String path, String city) {
        // load Tickets from path
        List<Ticket> localTickets;
        try {
            localTickets = Utils.loadTicketsFromPath(city,path);
            infractions = Utils.loadInfractionsFromPath(city,path);
            agencies = Utils.loadAgenciesFromPath(city,path);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // upload into the IList
        tickets.addAll(localTickets);
    }

    @Override
    public void run() {
        try{
            ICompletableFuture<List<String>> future = job
                    .mapper(new Query1Mapper())
                    .combiner(new Query1Combiner())
                    .reducer(new Query1Reducer())
                    .submit(new Query1Collator());
            results = future.get();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public String getResults() {
        StringBuilder sb = new StringBuilder();
        results.forEach(s -> sb.append(s).append("\n"));
        return sb.toString();
    }
}
