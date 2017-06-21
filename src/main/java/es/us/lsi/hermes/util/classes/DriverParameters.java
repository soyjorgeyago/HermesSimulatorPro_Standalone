package es.us.lsi.hermes.util.classes;

import java.io.Serializable;
import java.security.SecureRandom;
import org.supercsv.cellprocessor.ParseDouble;
import org.supercsv.cellprocessor.ift.CellProcessor;

public class DriverParameters implements Serializable {

    private double speedRandomFactor;
    private double hrRandomFactor;

    public DriverParameters() {
        final SecureRandom random = new SecureRandom();
        this.speedRandomFactor = 0.5d + (random.nextDouble() * 1.0d);
        this.hrRandomFactor = 0.9d + (random.nextDouble() * 0.2d);
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

    private static CellProcessor[] cellProcessors = new CellProcessor[]{new ParseDouble(), new ParseDouble()};
    private static String[] fields = new String[]{"SpeedRandomFactor", "HrRandomFactor"};
    private static String[] headers = new String[]{"speedRandomFactor", "hrRandomFactor"};
    
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
