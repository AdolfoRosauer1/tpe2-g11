package ar.edu.itba.pod.client.utils;

public enum Args {
    BATCH_SIZE("batchSize"),
    CITY("city"),
    FROM("from"),
    IN_PATH("inPath"),
    N("n"),
    OUT_PATH("outPath"),
    QUERY("query"),
    SERVER_ADDRESS("addresses"),
    TO("to"),
    AGENCY("agency");

    private final String value;

    Args(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
