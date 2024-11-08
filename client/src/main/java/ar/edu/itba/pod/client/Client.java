package ar.edu.itba.pod.client;

import ar.edu.itba.pod.client.queries.Query;
import ar.edu.itba.pod.client.queries.Query1;
import ar.edu.itba.pod.client.queries.Query3;
import ar.edu.itba.pod.client.queries.Query4;
import ar.edu.itba.pod.client.queries.Query2;
import ar.edu.itba.pod.client.utils.Args;
import ar.edu.itba.pod.client.utils.Utils;
import com.hazelcast.client.HazelcastClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.hazelcast.core.HazelcastInstance;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Map;

public class Client {
    public static Logger logger = LoggerFactory.getLogger(Client.class);

    public static void main(String[] args) throws InterruptedException {
        logger.info("tpe2-g11 Client Starting ...");
        logger.info("grpc-com-patterns Client Starting ...");

        Map<String,String> argMap = Utils.parseArgs(args);
        final String serverAddress = argMap.get(Args.SERVER_ADDRESS.getValue());
        if (serverAddress == null) {
            throw new IllegalArgumentException("Server address must be provided");
        }
        final String selectedQuery = argMap.get(Args.QUERY.getValue());
        if (selectedQuery == null) {
            throw new IllegalArgumentException("Query number must be provided");
        }
        final String inPath = argMap.get(Args.IN_PATH.getValue());
        if (inPath == null) {
            throw new IllegalArgumentException("Input path must be provided");
        }
        final String city = argMap.get(Args.CITY.getValue());
        if (city == null) {
            throw new IllegalArgumentException("City must be provided");
        }
        
        String agencyString = argMap.get(Args.AGENCY.getValue());
        String agency = null;
        if (agencyString != null) {
            agency = agencyString;
        }

        String nString = argMap.get(Args.N.getValue());
        int n = 0;
        if (nString != null) {
            n = Integer.parseInt(nString);
        }

        String fromString = argMap.get(Args.FROM.getValue());
        String toString = argMap.get(Args.TO.getValue());
        LocalDate from = null;
        LocalDate to = null;
        DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("dd/MM/yyyy");
        if (fromString != null) {
            from = LocalDate.parse(fromString, dateFormatter);
        }
        if (toString != null) {
            to = LocalDate.parse(toString, dateFormatter);
        }


        HazelcastInstance hazelcastInstance = Utils.getHazelcastInstance(serverAddress);

        try {
            Query query;
            switch (selectedQuery){
                case "1":
                    query = new Query1(hazelcastInstance);
                    break;
                case "2":
                    query = new Query2(hazelcastInstance);
                    break;
                case "3":
                    if (n < 2){
                        throw new IllegalArgumentException("n must be greater than 2");
                    }
                    query = new Query3(hazelcastInstance, from, to, n);
                    break;
                case "4":
                    query = new Query4(hazelcastInstance, agency, n);
                    break;
                default:
                    System.err.println("Unrecognized query: " + selectedQuery);
                    return;
            }
            long startLoadTime = System.currentTimeMillis();
            query.loadFromPath(inPath, city);
            long endLoadTime = System.currentTimeMillis();
            logger.info("Data loading completed in {} ms", (endLoadTime - startLoadTime));

            long startRunTime = System.currentTimeMillis();
            logger.info("Running query");
            query.run();
            long endRunTime = System.currentTimeMillis();
            logger.info("Query run completed in {} ms", (endRunTime - startRunTime));

            String result = query.getResults();
            logger.info("TOTAL runtime: {}", (endRunTime - startLoadTime));

            try {
                String outputPath = argMap.get(Args.OUT_PATH.getValue());
                Utils.saveResultsToFile(result, selectedQuery, outputPath);
                Utils.savePerformanceResults(selectedQuery, outputPath, 
                    startLoadTime, endLoadTime, 
                    startRunTime, endRunTime);
                Utils.savePerformanceResultsCSV(selectedQuery, inPath, outputPath,
                    startLoadTime, endLoadTime, 
                    startRunTime, endRunTime);
            } catch (IOException e) {
                logger.error("Error saving results to file", e);
            }

            
        } finally {

            HazelcastClient.shutdownAll();
        }
    }


}
