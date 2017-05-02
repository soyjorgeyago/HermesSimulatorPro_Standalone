package es.us.lsi.hermes.csv;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.supercsv.cellprocessor.ift.CellProcessor;

/**
 * Clase para monitorizar segund a segundo el estado de un SmartDriver.
 */
public class CSVSmartDriverStatus implements Serializable, ICSVBean {

    private int id;
    private final long timestamp;
    private final long delay;
    private final int size;

    protected CellProcessor[] cellProcessors;
    protected String[] fields;
    protected String[] headers;

    public CSVSmartDriverStatus() {
        this(0, System.currentTimeMillis(), 0, 0);
    }

    public CSVSmartDriverStatus(int id, long timestamp, long delay, int size) {
        this.id = id;
        this.timestamp = timestamp;
        this.delay = delay;
        this.size = size;
        init();
    }

    public void setFields(String[] fields) {
        this.fields = fields;
    }

    public void setHeaders(String[] headers) {
        this.headers = headers;
    }

    public int getId() {
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

    @Override
    public final void init() {

        cellProcessors = new CellProcessor[]{null, null, null, null};

        List<String> f = new ArrayList();

        f.add("id");
        f.add("timestamp");
        f.add("delay");
        f.add("size");

        fields = f.toArray(new String[f.size()]);

        List<String> h = new ArrayList();

        h.add("Id");
        h.add("Timestamp");
        h.add("Delay");
        h.add("Size");

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
