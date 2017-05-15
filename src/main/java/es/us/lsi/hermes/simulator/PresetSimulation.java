package es.us.lsi.hermes.simulator;

import es.us.lsi.hermes.util.Constants;
import es.us.lsi.hermes.util.StorageUtils;
import es.us.lsi.hermes.util.Util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PresetSimulation {

    private static final Logger LOG = Logger.getLogger(PresetSimulation.class.getName());

    private static final Properties PRESET_SIMULATION_PROPERTIES;

    private static int distanceFromCenter;
    private static int maxPathDistance;
    private static int pathsAmount;
    private static int driversByPath;
    private static int pathsGenerationMethod;
    private static int streamServer;
    private static int startingMode;
    private static boolean retryOnFail;
    private static int intervalBetweenRetriesInSeconds;
    private static Date scheduledSimulation;
    private static String sendResultsToEmail;
    private static boolean randomizeEachSmartDriverBehaviour;
    private static int retries;
    private static boolean useRoutesFromHdd;
    private static String pathForCsvStorage;

    static {
        LOG.log(Level.INFO, "PresetSimulation() - Preset configuration init.");

        PRESET_SIMULATION_PROPERTIES = Util.initProperties("PresetSimulation.properties");
        validate();
    }

    /**
     * Validates the 'PresetSimulation.properties' file. If invalid value is set
     * in any attribute, it will be used the default configuration.
     */
    private static void validate() {

        LOG.log(Level.INFO, "Validating 'PresetSimulation.properties'");

        distanceFromCenter = getIntValue("distance.from.center", 1, 100, 10);
        maxPathDistance = getIntValue("max.path.distance", 1, 100, 10);
        pathsAmount = getIntValue("paths.amount", 1, 10, 5);
        driversByPath = getIntValue("drivers.by.path", 1, 3000, 10);
        pathsGenerationMethod = getIntValue("paths.generation.method", 0, 1, 0);
        streamServer = getIntValue("stream.server", 0, 1, 0);
        startingMode = getIntValue("starting.mode", 0, 2, 1);
        retryOnFail = getBooleanValue("retry.on.fail", true);
        intervalBetweenRetriesInSeconds = getIntValue("interval.between.retries.s", 1, 60, 10);
        // FIXME: Check valid future schedules. If it is before now, it will start immediately.
        scheduledSimulation = getDateValue("scheduled.simulation", Constants.dfFile);
        sendResultsToEmail = getEmailValue("send.results.to.email", "jorgeyago.ingeniero@gmail.com");
        randomizeEachSmartDriverBehaviour = getBooleanValue("randomize.behaviour", true);
        retries = getIntValue("retries", -1, 5, 1);
        useRoutesFromHdd = getBooleanValue("use.routes.from.hdd", false);
        pathForCsvStorage = getPathValue("path.csv.storage", "CSV_storage");
    }

    private static String getPathValue(String propertyName, String defaultValue){
        String property = PRESET_SIMULATION_PROPERTIES.getProperty(propertyName);

        if (!StorageUtils.canWrite(property)) {
            property = defaultValue;
            LOG.log(Level.SEVERE, "{0} property not declared or not writable, using default: {1}", new Object[]{propertyName, property});
        }
        return property;
    }

    private static int getIntValue(String propertyName, int minimum, int maximum, int defaultValue) {
        String property = PRESET_SIMULATION_PROPERTIES.getProperty(propertyName, String.valueOf(defaultValue));
        int value = defaultValue;

        try {
            int v = Integer.parseInt(property);
            if (v < minimum || v > maximum) {
                LOG.log(Level.SEVERE, "validate() - Invalid value for {0}. Should be [{1} to {2}] - Using default value: {3}", new Object[]{propertyName, minimum, maximum, defaultValue});
            } else {
                value = v;
            }
        } catch (NumberFormatException ex) {
            LOG.log(Level.SEVERE, "validate() - Invalid value for {0}. Should be [{1} to {2}] - Using default value: {3}", new Object[]{propertyName, minimum, maximum, defaultValue});
        }

        return value;
    }

    private static boolean getBooleanValue(String propertyName, boolean defaultValue) {
        String property = PRESET_SIMULATION_PROPERTIES.getProperty(propertyName, String.valueOf(defaultValue));
        return Boolean.parseBoolean(property);
    }

    private static Date getDateValue(String propertyName, SimpleDateFormat sdf) {
        String property = PRESET_SIMULATION_PROPERTIES.getProperty(propertyName);
        Date value = null;

        if (property != null) {
            try {
                value = sdf.parse(property);
            } catch (ParseException ex) {
                LOG.log(Level.SEVERE, "validate() - Invalid date format for {0}. Should be [yyyy-MM-dd_HH.mm.ss]", propertyName);
            }
        }

        return value;
    }

    private static String getEmailValue(String propertyName, String defaultValue) {
        String property = PRESET_SIMULATION_PROPERTIES.getProperty(propertyName, defaultValue);

        if (!Util.isValidEmail(property)) {
            LOG.log(Level.SEVERE, "validate() - Invalid e-mail format for {0}. Should be a valid e-mail. Using default");
            return null;
        }

        return property;
    }

    public static int getDistanceFromCenter() {
        return distanceFromCenter;
    }

    public static int getMaxPathDistance() {
        return maxPathDistance;
    }

    public static int getPathsAmount() {
        return pathsAmount;
    }

    public static int getDriversByPath() {
        return driversByPath;
    }

    public static int getPathsGenerationMethod() {
        return pathsGenerationMethod;
    }

    public static int getStreamServer() {
        return streamServer;
    }

    public static int getStartingMode() {
        return startingMode;
    }

    public static boolean isRetryOnFail() {
        return retryOnFail;
    }

    public static int getIntervalBetweenRetriesInSeconds() {
        return intervalBetweenRetriesInSeconds;
    }

    public static Date getScheduledSimulation() {
        return scheduledSimulation;
    }

    public static String getSendResultsToEmail() {
        return sendResultsToEmail;
    }

    public static boolean isRandomizeEachSmartDriverBehaviour() {
        return randomizeEachSmartDriverBehaviour;
    }

    public static int getRetries() {
        return retries;
    }

    public static boolean isUseRoutesFromHdd() {
        return useRoutesFromHdd;
    }

    public static String getPathForCsvStorage() {
        return pathForCsvStorage;
    }
}
