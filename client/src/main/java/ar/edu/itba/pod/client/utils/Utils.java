package ar.edu.itba.pod.client.utils;

import ar.edu.itba.pod.models.ticket.Ticket;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.HazelcastInstance;

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
import java.util.stream.Stream;

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

    public static List<Ticket> loadTicketsFromPath(String city, String path) throws IOException {
        List<Ticket> toReturn = new ArrayList<>();
        switch (city) {
            case "CHI":
                // Headers ticketsCHI.csv:
                // issue_date;community_area_name;unit_description;license_plate_number;violation_code;fine_amount
                try (Stream<String> lines = Files.lines(Paths.get(path))) {
                    // Define the date format used in the CSV file
                    DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("MM/dd/yyyy");

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
                                        Integer infractionCode = Integer.parseInt(infractionCodeStr);

                                        // Parse the fine amount
                                        Double fineAmount = Double.parseDouble(fineAmountStr);

                                        // Create the Ticket object
                                        Ticket ticket = new Ticket(licensePlate, issueDate, infractionCode, fineAmount,
                                                agency, region);
                                        toReturn.add(ticket);
                                    } catch (Exception e) {
                                        System.err.println("Error parsing line: " + String.join(";", parts));
                                    }
                                } else {
                                    System.err.println("Invalid line format: " + String.join(";", parts));
                                }
                            });
                }
                break;

            case "NYC":
                // Headers ticketsNYC.csv:
                // Plate;Infraction ID;Fine Amount;Issuing Agency;Issue Date;County Name
                try (Stream<String> lines = Files.lines(Paths.get(path))) {
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
                                        Integer infractionCode = Integer.parseInt(infractionCodeStr);

                                        // Parse the fine amount
                                        Double fineAmount = Double.parseDouble(fineAmountStr);

                                        // Create the Ticket object
                                        Ticket ticket = new Ticket(licensePlate, issueDate, infractionCode, fineAmount,
                                                agency, region);
                                        toReturn.add(ticket);
                                    } catch (Exception e) {
                                        System.err.println("Error parsing line: " + String.join(";", parts));
                                    }
                                } else {
                                    System.err.println("Invalid line format: " + String.join(";", parts));
                                }
                            });
                }
                break;

            default:
                System.err.println("Invalid city code.");
        }

        return toReturn;
    }

    public static List<String> loadAgenciesFromPath(String path, String city) throws IOException{
        List<String> toReturn = new ArrayList<>();

        // infractions.csv headers: code;name
        try (var lines = Files.lines(Path.of(path + "/agencies" + city + ".csv"))) {
            lines.skip(1)
                    .forEach(toReturn::add);
        }

        return toReturn;
    }


    public static Map<Integer, String> loadInfractionsFromPath(String path, String city) throws IOException {
        Map<Integer, String> toReturn = new HashMap<>();

        // infractions.csv headers: code;name
        try (var lines = Files.lines(Path.of(path + "/infractions" + city + ".csv"))) {
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
