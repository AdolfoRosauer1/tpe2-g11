package ar.edu.itba.pod.client.utils;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.HazelcastInstance;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    public static void loadInfractionsFromPath(){

    }

}
