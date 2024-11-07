package ar.edu.itba.pod.client;

import ar.edu.itba.pod.client.queries.Query;
import ar.edu.itba.pod.client.queries.Query1;
import ar.edu.itba.pod.client.queries.Query3;
import ar.edu.itba.pod.client.queries.Query4;
import ar.edu.itba.pod.client.utils.Args;
import ar.edu.itba.pod.client.utils.Utils;
import com.hazelcast.client.HazelcastClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.hazelcast.core.HazelcastInstance;
import java.util.Map;

public class Client {
    public static Logger logger = LoggerFactory.getLogger(Client.class);

    public static void main(String[] args) throws InterruptedException {
        logger.info("tpe2-g11 Client Starting ...");
        logger.info("grpc-com-patterns Client Starting ...");

        Map<String,String> argMap = Utils.parseArgs(args);
        final String serverAddress = argMap.get(Args.SERVER_ADDRESS.getValue());
        final String selectedQuery = argMap.get(Args.QUERY.getValue());
        final String inPath = argMap.get(Args.IN_PATH.getValue());
        final String city = argMap.get(Args.CITY.getValue());

        HazelcastInstance hazelcastInstance = Utils.getHazelcastInstance(serverAddress);

        logger.info("Args {}", argMap);
        logger.info("Selected query: {}", selectedQuery);
        try {
            Query query;
            switch (selectedQuery){
                case "1":
                    query = new Query1(hazelcastInstance);
                    break;
//                case "2":
////                    query = new Query2(hazelcastInstance);
//                    break;
                case "3":
                    // TODO: argumentos para Query3 por constructor
                    query = new Query3(hazelcastInstance);
                    break;
                case "4":
                    // TODO: argumentos para Query4 por constructor
                    query = new Query4(hazelcastInstance);
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
            System.out.println(result);
            logger.info("TOTAL runtime: {}", (endRunTime - startLoadTime));
        } finally {

            HazelcastClient.shutdownAll();
        }
    }
}
