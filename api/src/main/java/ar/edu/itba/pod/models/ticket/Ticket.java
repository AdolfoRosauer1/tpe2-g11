package ar.edu.itba.pod.models.ticket;

import java.time.LocalDate;

public abstract class Ticket {
    final String licensePlate;
    final LocalDate issueDate;
    final String infractionCode;
    final int fineAmount;
    final String agency;
    final String region;

    protected Ticket(String licensePlate, LocalDate issueDate, String infractionCode, int fineAmount, String agency, String region) {
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

    public Integer getFineAmount() {
        return fineAmount;
    }

    public String getAgency() {
        return agency;
    }

    public String getRegion() {
        return region;
    }
}
