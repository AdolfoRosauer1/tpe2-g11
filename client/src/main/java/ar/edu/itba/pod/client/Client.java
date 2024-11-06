package ar.edu.itba.pod.client;

import ar.edu.itba.pod.client.queries.Query1;
import ar.edu.itba.pod.client.utils.Args;
import ar.edu.itba.pod.client.utils.Utils;
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
        final String query = argMap.get(Args.QUERY.getValue());
        final String inPath = argMap.get(Args.IN_PATH.getValue());

        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 50051)
                .usePlaintext()
                .build();

        HazelcastInstance hazelcastInstance = Utils.getHazelcastInstance(serverAddress);

        try {
            switch (query){
                case "Query1":
                    // First load data

                    // Then run query
                    Query1 client = new Query1(hazelcastInstance);
                    client.loadFromPath(inPath);
                    client.run();
                    client.getResults();
            }
        } finally {
            channel.shutdown().awaitTermination(10, TimeUnit.SECONDS);
        }
    }
}
