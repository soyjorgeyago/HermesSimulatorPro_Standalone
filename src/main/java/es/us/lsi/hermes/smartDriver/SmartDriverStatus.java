package es.us.lsi.hermes.smartDriver;

import java.io.Serializable;

/**
 * SmartDriver status information.
 */
public class SmartDriverStatus implements Serializable {

    // TODO: Is it is being sent the SHA through the key value in the Producer, remove from these attributes.
    private String sha;
    private final long timestamp;
    private final long delay;
    private final int size;

    public SmartDriverStatus() {
        this("", System.currentTimeMillis(), 0, 0);
    }

    public SmartDriverStatus(String sha, long timestamp, long delay, int size) {
        this.sha = sha;
        this.timestamp = timestamp;
        this.delay = delay;
        this.size = size;
    }

    public String getSha() {
        return sha;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public long getDelay() {
        return delay;
    }

    public int getSize() {
        return size;
    }
}
