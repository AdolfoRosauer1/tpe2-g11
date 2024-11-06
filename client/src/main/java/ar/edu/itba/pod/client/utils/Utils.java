package ar.edu.itba.pod.client.utils;

import ar.edu.itba.pod.models.ticket.Infraction;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.HazelcastInstance;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 *
 */
public class Utils {

    public static Map<String,String> parseArgs(String[] args) {
        Map<String,String> map = new HashMap<>();
        for (String arg : args) {
            String[] split = arg.substring(2).split("=");
            if (split.length == 2) {
                map.put(split[0], split[1]);
            }
        }
        return map;
    }

    public static HazelcastInstance getHazelcastInstance(String addresses){
        GroupConfig groupConfig = new GroupConfig()
                .setName("g8")
                .setPassword("g8-pass");


        List<String> serverAddresses = List.of(addresses.split(";"));
        ClientNetworkConfig clientNetworkConfig = new ClientNetworkConfig().setAddresses(serverAddresses);


        ClientConfig clientConfig = new ClientConfig()
                .setGroupConfig(groupConfig)
                .setNetworkConfig(clientNetworkConfig);

        HazelcastInstance instance = HazelcastClient.newHazelcastClient(clientConfig);

        return instance;
    }

    public static void loadTicketsFromPath(){

    }

    public static void loadAgenciesFromPath(){

    }

    public static Map<Integer, String> loadInfractionsFromPath(String path) throws IOException {
        Map<Integer, String> toReturn = new HashMap<>();

        // infractions.csv headers: code;name
        try (var lines = Files.lines(Path.of(path))) {
            lines.skip(1)
                    .map(line -> line.split(";"))
                    .forEach(parts -> {
                        if (parts.length >= 2) {
                            try {
                                int code = Integer.parseInt(parts[0].trim());
                                String name = parts[1].trim();
                                toReturn.put(code, name);
                            } catch (NumberFormatException e) {
                                System.err.println("Invalid code format: " + parts[0]);
                            }
                        } else {
                            System.err.println("Invalid line format: " + String.join(";", parts));
                        }
                    });
        }

        return toReturn;
    }


}
