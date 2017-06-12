package es.us.lsi.hermes.csv;

import java.io.Serializable;
import org.supercsv.cellprocessor.ift.CellProcessor;

/**
 * Clase con los campos del evento que serán exportados al CSV de resumen de
 * tramas enviadas.
 */
public class CSVEvent implements Serializable {

    private final String eventId;
    private final String timestamp;

    public CSVEvent() {
        this(null, null);
    }

    public CSVEvent(String eventId, String timestamp) {
        this.eventId = eventId;
        this.timestamp = timestamp;
    }

    public String getEventId() {
        return eventId;
    }

    public String getTimestamp() {
        return timestamp;
    }

    private static CellProcessor[] cellProcessors = new CellProcessor[]{new org.supercsv.cellprocessor.constraint.NotNull(), new org.supercsv.cellprocessor.constraint.NotNull()};
    private static String[] fields = new String[]{"eventId", "timestamp"};
    private static String[] headers = new String[]{"Event-Id", "Timestamp"};

    public static CellProcessor[] getProcessors() {
        return cellProcessors;
    }

    public static String[] getFields() {
        return fields;
    }

    public static String[] getHeaders() {
        return headers;
    }
}
