package es.us.lsi.hermes.util;

import es.us.lsi.hermes.csv.ICSVBean;
import java.security.SecureRandom;
import org.supercsv.cellprocessor.ParseDouble;
import org.supercsv.cellprocessor.ift.CellProcessor;

public class DriverParameters implements ICSVBean {

    private double speedRandomFactor;
    private double hrRandomFactor;

    public DriverParameters() {
        final SecureRandom random = new SecureRandom();
        this.speedRandomFactor = 0.5d + (random.nextDouble() * 1.0d);
        this.hrRandomFactor = 0.9d + (random.nextDouble() * 0.2d);
        
        initCSV();
    }

    public DriverParameters(double speedRandomFactor, double hrRandomFactor) {
        this.speedRandomFactor = speedRandomFactor;
        this.hrRandomFactor = hrRandomFactor;

        initCSV();
    }

    public double getSpeedRandomFactor() {
        return speedRandomFactor;
    }

    public double getHrRandomFactor() {
        return hrRandomFactor;
    }

    public void setSpeedRandomFactor(double speedRandomFactor) {
        this.speedRandomFactor = speedRandomFactor;
    }

    public void setHrRandomFactor(double hrRandomFactor) {
        this.hrRandomFactor = hrRandomFactor;
    }

    // ------------------------- CSV IMP/EXP -------------------------
    private CellProcessor[] cellProcessors;
    private String[] fields;
    private String[] headers;

    @Override
    public final void initCSV() {
        cellProcessors = new CellProcessor[]{new ParseDouble(), new ParseDouble()};

        headers = new String[]{"SpeedRandomFactor", "HrRandomFactor"};

        fields = new String[]{"speedRandomFactor", "hrRandomFactor"};
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
