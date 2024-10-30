package ar.edu.itba.pod.client.queries;

public interface Query {
    void loadFromPath(String path);
    void run();
    String getResults();
}
