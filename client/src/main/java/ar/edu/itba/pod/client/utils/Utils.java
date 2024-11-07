package ar.edu.itba.pod.client.utils;

import ar.edu.itba.pod.models.ticket.Ticket;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static ar.edu.itba.pod.client.Client.logger;

public class Utils {

    private static final int BATCH_SIZE = 1000;

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
                .setName("g11")
                .setPassword("g11-pass");


        List<String> serverAddresses = List.of(addresses.split(";"));
        ClientNetworkConfig clientNetworkConfig = new ClientNetworkConfig().setAddresses(serverAddresses);


        ClientConfig clientConfig = new ClientConfig()
                .setGroupConfig(groupConfig)
                .setNetworkConfig(clientNetworkConfig);

        HazelcastInstance instance = HazelcastClient.newHazelcastClient(clientConfig);

        return instance;
    }

    public static void loadTicketsFromPathAndUpload( String directory, String city, IMap<Long, Ticket> distributedMap) throws IOException {
        logger.info("loadingTicketsFromPathAndUpload "+ directory+ "\t" + city);
        logger.info("Current working directory: " + System.getProperty("user.dir"));
        Path currentPath = Paths.get(".").toAbsolutePath().normalize();
        logger.info("Interpreted path for '.': " + currentPath);

        final AtomicInteger batchCounter = new AtomicInteger(0);
        Map<Long, Ticket> toLoad = new HashMap<>();

        final AtomicLong counter = new AtomicLong(0);
        String path = directory + "/tickets"+city+".csv";
        final Path ticketsPath = Paths.get(path);
        switch (city) {
            case "CHI" -> {
                // Headers ticketsCHI.csv:
                // issue_date;community_area_name;unit_description;license_plate_number;violation_code;fine_amount
                try (Stream<String> lines = Files.lines(ticketsPath)) {
                    // Define the date format used in the CSV file
                    DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

                    lines.skip(1) // Skip the header line
                            .map(line -> line.split(";"))
                            .forEach(parts -> {
                                if (parts.length >= 6) {
                                    try {
                                        String issueDateStr = parts[0].trim();
                                        String region = parts[1].trim(); // community_area_name
                                        String agency = parts[2].trim(); // unit_description
                                        String licensePlate = parts[3].trim(); // license_plate_number
                                        String infractionCodeStr = parts[4].trim(); // violation_code
                                        String fineAmountStr = parts[5].trim(); // fine_amount

                                        // Parse the issue date
                                        LocalDate issueDate = LocalDate.parse(issueDateStr, dateFormatter);

                                        // Parse the infraction code
//                                        Integer infractionCode = Integer.parseInt(infractionCodeStr);

                                        // Parse the fine amount
                                        Double fineAmount = Double.parseDouble(fineAmountStr);

                                        // Create the Ticket object
                                        Ticket ticket = new Ticket(licensePlate, issueDate, infractionCodeStr, fineAmount, agency, region);
                                        toLoad.put(counter.getAndIncrement(), ticket);
                                    } catch (Exception e) {
                                        System.err.println("Error parsing line: " + String.join(";", parts) +"\n"+e.getMessage());
                                    }
                                } else {
                                    System.err.println("Invalid line format: " + String.join(";", parts));
                                }

                                if (batchCounter.incrementAndGet() >= BATCH_SIZE) {
                                    distributedMap.putAll(toLoad);
                                    toLoad.clear();
                                    batchCounter.set(0);
                                }
                            });
                    if (batchCounter.get() > 0) {
                        distributedMap.putAll(toLoad);
                        toLoad.clear();
                        batchCounter.set(0);
                    }
                }
            }
            case "NYC" -> {
                // Headers ticketsNYC.csv:
                // Plate;Infraction ID;Fine Amount;Issuing Agency;Issue Date;County Name
                try (Stream<String> lines = Files.lines(ticketsPath)) {
                    // Define the date format used in the CSV file
                    DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

                    lines.skip(1) // Skip the header line
                            .map(line -> line.split(";"))
                            .forEach(parts -> {
                                if (parts.length >= 6) {
                                    try {
                                        String licensePlate = parts[0].trim(); // Plate
                                        String infractionCodeStr = parts[1].trim(); // Infraction ID
                                        String fineAmountStr = parts[2].trim(); // Fine Amount
                                        String agency = parts[3].trim(); // Issuing Agency
                                        String issueDateStr = parts[4].trim(); // Issue Date
                                        String region = parts[5].trim(); // County Name

                                        // Parse the issue date
                                        LocalDate issueDate = LocalDate.parse(issueDateStr, dateFormatter);

                                        // Parse the infraction code
//                                        Integer infractionCode = Integer.parseInt(infractionCodeStr);

                                        // Parse the fine amount
                                        Double fineAmount = Double.parseDouble(fineAmountStr);

                                        // Create the Ticket object
                                        Ticket ticket = new Ticket(licensePlate, issueDate, infractionCodeStr, fineAmount,
                                                agency, region);
                                        toLoad.put(counter.getAndIncrement(), ticket);
                                    } catch (Exception e) {
                                        System.err.println("Error parsing line: " + String.join(";", parts));
                                    }
                                } else {
                                    System.err.println("Invalid line format: " + String.join(";", parts));
                                }
                                if (batchCounter.incrementAndGet() >= BATCH_SIZE) {
                                    distributedMap.putAll(toLoad);
                                    toLoad.clear();
                                    batchCounter.set(0);
                                }
                            });
                    if (batchCounter.get() > 0) {
                        distributedMap.putAll(toLoad);
                        toLoad.clear();
                        batchCounter.set(0);
                    }
                }
            }
            default -> System.err.println("Invalid city code.");
        }
    }

    public static List<String> loadAgenciesFromPath(String directory, String city) throws IOException{
        List<String> toReturn = new ArrayList<>();
        logger.info("LoadingAgenciesFromPath: path {} \t city {}", directory, city);

        // infractions.csv headers: code;name
        try (var lines = Files.lines(Path.of(directory + "/agencies" + city + ".csv"))) {
            lines.skip(1)
                    .forEach(toReturn::add);
        }
        logger.info("Successfully loaded agencies");
        return toReturn;
    }


    public static Map<String, String> loadInfractionsFromPath(String directory, String city) throws IOException {
        Map<String, String> toReturn = new HashMap<>();
        logger.info("loadInfractionsFromPath: directory {}\t city {}", directory, city);

        // infractions.csv headers: code;name
        try (var lines = Files.lines(Path.of(directory + "/infractions" + city + ".csv"))) {
            lines.skip(1)
                    .map(line -> line.split(";"))
                    .forEach(parts -> {
                        if (parts.length >= 2) {
                            try {
                                String code = parts[0].trim();
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
        logger.info("successfully loaded infractions from directory");
        return toReturn;
    }


}
