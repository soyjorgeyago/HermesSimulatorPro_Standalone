package es.us.lsi.hermes.smartDriver;

import java.io.Serializable;

/**
 * SmartDriver status information.
 */
public class SmartDriverStatus implements Serializable {

    private long id;
    private final long timestamp;
    private final long delay;
    private final int size;

    public SmartDriverStatus() {
        this(0l, System.currentTimeMillis(), 0, 0);
    }

    public SmartDriverStatus(long id, long timestamp, long delay, int size) {
        this.id = id;
        this.timestamp = timestamp;
        this.delay = delay;
        this.size = size;
    }

    public long getId() {
        return id;
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
