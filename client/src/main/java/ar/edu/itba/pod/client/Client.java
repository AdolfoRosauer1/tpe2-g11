package ar.edu.itba.pod.client;

import ar.edu.itba.pod.client.queries.Query;
import ar.edu.itba.pod.client.queries.Query1;
import ar.edu.itba.pod.client.utils.Args;
import ar.edu.itba.pod.client.utils.Utils;
import com.hazelcast.client.HazelcastClient;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.hazelcast.core.HazelcastInstance;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class Client {
    private static Logger logger = LoggerFactory.getLogger(Client.class);

    public static void main(String[] args) throws InterruptedException {
        logger.info("tpe2-g11 Client Starting ...");
        logger.info("grpc-com-patterns Client Starting ...");

        Map<String,String> argMap = Utils.parseArgs(args);
        final String serverAddress = argMap.get(Args.SERVER_ADDRESS.getValue());
        final String selectedQuery = argMap.get(Args.QUERY.getValue());
        final String inPath = argMap.get(Args.IN_PATH.getValue());
        final String city = argMap.get(Args.CITY.getValue());

        HazelcastInstance hazelcastInstance = Utils.getHazelcastInstance(serverAddress);

        try {
            switch (selectedQuery){
                case "Query1":
                    // First load data
                    // Then run query
                    Query query = new Query1(hazelcastInstance);
                    query.loadFromPath(inPath, city);
                    query.run();
                    String result = query.getResults();
                    System.out.println(result);
            }
        } finally {
            HazelcastClient.shutdownAll();
        }
    }
}
