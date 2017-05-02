package es.us.lsi.hermes.csv;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.supercsv.cellprocessor.ift.CellProcessor;

/**
 * Clase con el estado de la simulación en cada segundo.
 */
public class CSVSimulatorStatus implements Serializable, ICSVBean {

    private final long timestamp;
    private final int generated;
    private final int sent;
    private final int ok;
    private final int notOk;
    private final int errors;
    private final int recovered;
    private final int pending;
    private final int runningThreads;
    private final long maxSmartDriversDelay;
    private final long currentSmartDriversDelay;

    protected CellProcessor[] cellProcessors;
    protected String[] fields;
    protected String[] headers;

    public CSVSimulatorStatus() {
        this(System.currentTimeMillis(), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    }

    public CSVSimulatorStatus(long timestamp, int generated, int sent, int ok, int notOk, int errors, int recovered, int pending, int runningThreads, long maxSmartDriversDelay, long currentSmartDriversDelay) {
        this.timestamp = timestamp;
        this.generated = generated;
        this.sent = sent;
        this.ok = ok;
        this.notOk = notOk;
        this.errors = errors;
        this.recovered = recovered;
        this.pending = pending;
        this.runningThreads = runningThreads;
        this.maxSmartDriversDelay = maxSmartDriversDelay;
        this.currentSmartDriversDelay = currentSmartDriversDelay;
        init();
    }

    public void setFields(String[] fields) {
        this.fields = fields;
    }

    public void setHeaders(String[] headers) {
        this.headers = headers;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public int getGenerated() {
        return generated;
    }

    public int getSent() {
        return sent;
    }

    public int getOk() {
        return ok;
    }

    public int getNotOk() {
        return notOk;
    }

    public int getErrors() {
        return errors;
    }

    public int getRecovered() {
        return recovered;
    }

    public int getPending() {
        return pending;
    }

    public int getRunningThreads() {
        return runningThreads;
    }

    public long getMaxSmartDriversDelay() {
        return maxSmartDriversDelay;
    }

    public long getCurrentSmartDriversDelay() {
        return currentSmartDriversDelay;
    }

    @Override
    public final void init() {

        cellProcessors = new CellProcessor[]{null, null, null, null, null, null, null, null, null, null, null};

        List<String> f = new ArrayList();

        f.add("timestamp");
        f.add("generated");
        f.add("sent");
        f.add("ok");
        f.add("notOk");
        f.add("errors");
        f.add("recovered");
        f.add("pending");
        f.add("runningThreads");
        f.add("maxSmartDriversDelay");
        f.add("currentSmartDriversDelay");

        fields = f.toArray(new String[f.size()]);

        List<String> h = new ArrayList();

        h.add("Timestamp");
        h.add("Generated");
        h.add("Sent");
        h.add("Ok");
        h.add("Not Ok");
        h.add("Errors");
        h.add("Recovered");
        h.add("Pending");
        h.add("Running Threads");
        h.add("Max SmartDrivers delay");
        h.add("Current SmartDrivers delay");

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
