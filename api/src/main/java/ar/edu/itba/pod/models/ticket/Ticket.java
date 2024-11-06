package ar.edu.itba.pod.models.ticket;

import java.time.LocalDate;

public class Ticket {
    final String licensePlate;
    final LocalDate issueDate;
    final Integer infractionCode;
    final Double fineAmount;
    final String agency;
    final String region;

    public Ticket(String licensePlate, LocalDate issueDate, Integer infractionCode, Double fineAmount, String agency, String region) {
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

    public Integer getInfractionCode() {
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
