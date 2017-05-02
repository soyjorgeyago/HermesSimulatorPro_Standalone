package es.us.lsi.hermes.csv;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.supercsv.cellprocessor.ift.CellProcessor;

/**
 * Clase con los campos del evento que serán exportados al CSV de resumen de tramas enviadas.
 */
public class CSVEvent implements Serializable, ICSVBean {

    private final String eventId;
    private final String timestamp;

    protected CellProcessor[] cellProcessors;
    protected String[] fields;
    protected String[] headers;
    
    public CSVEvent() {
       this(null, null);
    }
    
    public CSVEvent(String eventId, String timestamp) {
        this.eventId = eventId;
        this.timestamp = timestamp;
        init();
    }

    public void setFields(String[] fields) {
        this.fields = fields;
    }

    public void setHeaders(String[] headers) {
        this.headers = headers;
    }

    public String getEventId() {
        return eventId;
    }

    public String getTimestamp() {
        return timestamp;
    }

    @Override
    public final void init() {
        List<CellProcessor> cpl = new ArrayList<>();

        cpl.add(new org.supercsv.cellprocessor.constraint.NotNull()); // Timestamp del evento.
        cpl.add(new org.supercsv.cellprocessor.constraint.NotNull()); // Identificador del evento.

        cellProcessors = cpl.toArray(new CellProcessor[cpl.size()]);

        List<String> f = new ArrayList();

        f.add("eventId");
        f.add("timestamp");

        fields = f.toArray(new String[f.size()]);

        List<String> h = new ArrayList();

        h.add("Event-Id");
        h.add("Timestamp");

        headers = h.toArray(new String[h.size()]);
    }

    public CellProcessor[] getCellProcessors() {
        return cellProcessors;
    }

    public void setCellProcessors(CellProcessor[] cellProcessors) {
        this.cellProcessors = cellProcessors;
    }

    @Override
    public CellProcessor[] getProcessors() {
        return cellProcessors;
    }

    @Override
    public String[] getFields() {
        return fields;
    }

    @Override
    public String[] getHeaders() {
        return headers;
    }
}
