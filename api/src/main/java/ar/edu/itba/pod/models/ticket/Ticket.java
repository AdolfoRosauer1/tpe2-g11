package ar.edu.itba.pod.models.ticket;

import java.io.Serializable;
import java.time.LocalDate;

public class Ticket implements Serializable {
    private final String licensePlate;
    private final LocalDate issueDate;
    private final String infractionCode;
    private final Double fineAmount;
    private final String agency;
    private final String region;

    public Ticket(String licensePlate, LocalDate issueDate, String infractionCode, Double fineAmount, String agency, String region) {
        this.licensePlate = licensePlate;
        this.issueDate = issueDate;
        this.infractionCode = infractionCode;
        this.fineAmount = fineAmount;
        this.agency = agency;
        this.region = region;
    }

    public String getLicensePlate() {
        return licensePlate;
    }

    public LocalDate getIssueDate() {
        return issueDate;
    }

    public String getInfractionCode() {
        return infractionCode;
    }

    public Double getFineAmount() {
        return fineAmount;
    }

    public String getAgency() {
        return agency;
    }

    public String getRegion() {
        return region;
    }
}
