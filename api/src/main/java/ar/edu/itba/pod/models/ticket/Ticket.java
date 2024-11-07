package ar.edu.itba.pod.models.ticket;

import java.io.Serializable;
import java.time.LocalDate;

public record Ticket(String licensePlate, LocalDate issueDate, String infractionCode, Double fineAmount, String agency,
                     String region) implements Serializable {
}
