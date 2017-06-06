package es.us.lsi.hermes.util;

import es.us.lsi.hermes.csv.ICSVBean;
import org.supercsv.cellprocessor.ParseDouble;
import org.supercsv.cellprocessor.ift.CellProcessor;

public class DriverParameters implements ICSVBean {

    private double speedRandomFactor;
    private double hrRandomFactor;

    public DriverParameters() {
        speedRandomFactor = 0.0d;
        hrRandomFactor = 0.0d;
    }

    public DriverParameters(double speedRandomFactor, double hrRandomFactor) {
        this.speedRandomFactor = speedRandomFactor;
        this.hrRandomFactor = hrRandomFactor;
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
    public void initCSV() {
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
