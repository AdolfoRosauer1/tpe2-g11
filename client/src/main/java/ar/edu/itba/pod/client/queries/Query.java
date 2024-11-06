package ar.edu.itba.pod.client.queries;

public interface Query {
    void loadFromPath(String path, String city);
    void run();
    String getResults();
}
