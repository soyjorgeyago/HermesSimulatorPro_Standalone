package es.us.lsi.hermes.util;

import es.us.lsi.hermes.google.directions.Location;

import java.text.DecimalFormat;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.ResourceBundle;
import java.util.logging.Logger;

public final class Constants {

    private Constants() { }

    private static final Logger LOG = Logger.getLogger(Constants.class.getName());

    private static final ResourceBundle BUNDLE = ResourceBundle.getBundle("Bundle");

    // Application name for events send.
    public static final String SIMULATOR_APPLICATION_ID = "SmartDriver";

    // Zip fragment file size.
    public static final int ZIP_FILE_SIZE = 26214400;

    // Date and time formatting.
    public static final SimpleDateFormat df = new SimpleDateFormat("dd/MM/yyyy");
    public static final SimpleDateFormat dfTime = new SimpleDateFormat("HH:mm:ss");
    public static final SimpleDateFormat dfISO8601 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    public static final DecimalFormat df2Decimals = new DecimalFormat("0.00");
    public static final SimpleDateFormat dfFile = new SimpleDateFormat("yyyy-MM-dd_HH.mm.ss");

    // TODO: Relocate in PresetSimulation.properties.
    public static final Location SEVILLE = new Location(37.3898358, -5.986069);

    // Equivalent to resting mean heart rate of 70 bpm.
    public static final int RR_TIME = 850;

    // SmartDriver starting maximum delay, in milliseconds.
    public static final int MAX_INITIAL_DELAY = 60000;

    // Path generation request amount per second. It is a Google limitation.
    public static final int REQUEST_PACK_SIZE = 10;

    public enum Paths_Generation_Method {
        GOOGLE, OPENSTREETMAP
    }
    
    public static String getBundleValue(String key) {
        return BUNDLE.getString(key);
    }

    public static String getBundleValue(String key, Object... params) {
        return MessageFormat.format(BUNDLE.getString(key), params);
    }

    // Simulated Smart Driver ----------------------------------------------

    // Parámetros recogidos de SmartDriver.
    public static final int SEND_INTERVAL_SECONDS = 10;
    public static final int SEND_INTERVAL_METERS = 500;
    public static final double HIGH_ACCELERATION_THRESHOLD = 2.5d;
    public static final double HIGH_DECELERATION_THRESHOLD = -3.5d;

    public static final String DATA_SECTION = "Data Section";
    public static final String VEHICLE_LOCATION = "Vehicle Location";
}
