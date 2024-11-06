package ar.edu.itba.pod.models.ticket;

public class Infraction {

    private final Integer id;
    private final String infractionName;

    public Infraction(Integer id, String infractionName) {
        this.id = id;
        this.infractionName = infractionName;
    }

    public Integer getId() {
        return id;
    }

    public String getInfractionName() {
        return infractionName;
    }
}
