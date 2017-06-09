package es.us.lsi.hermes.simulator;

import es.us.lsi.hermes.util.Constants;
import es.us.lsi.hermes.util.StorageUtils;
import es.us.lsi.hermes.util.Util;
import java.text.MessageFormat;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang.time.DurationFormatUtils;

public class PresetSimulation {

    private static final Logger LOG = Logger.getLogger(PresetSimulation.class.getName());

    private static final Properties PRESET_SIMULATION_PROPERTIES;

    private static int distanceFromCenter;
    private static int maxPathDistance;
    private static int pathsAmount;
    private static int driversByPath;
    private static int pathsGenerationMethod;
    private static Constants.SmartDrivers_Starting_Mode startingMode;
    private static boolean retryOnFail;
    private static int intervalBetweenRetriesInSeconds;
    private static Date scheduledSimulation;
    private static String sendResultsToEmail;
    private static boolean randomizeEachSmartDriverBehaviour;
    private static int retries;
    private static boolean loadPathsAndDriversFromHdd;
    private static String csvStorageFolder;
    private static int maxResponseDelayMs;
    private static int maxSimulationTimeInSeconds;
    private static String maxSimulationTimeStringFormatted;
    private static int statusSamplingIntervalInSeconds;
    private static int consumerPollTimeout;
    private static boolean loopingSimulation;
    private static boolean kafkaProducerPerSmartDriver;

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
        int sm = getIntValue("starting.mode", 0, 2, 1);
        startingMode = Constants.SmartDrivers_Starting_Mode.values()[sm];
        retryOnFail = getBooleanValue("retry.on.fail", true);
        intervalBetweenRetriesInSeconds = getIntValue("interval.between.retries.s", 1, 60, 10);
        scheduledSimulation = getDateValue("scheduled.simulation", Constants.dfFile);
        sendResultsToEmail = getEmailValue("send.results.to.email", "jorgeyago.ingeniero@gmail.com");
        randomizeEachSmartDriverBehaviour = getBooleanValue("randomize.behaviour", true);
        retries = getIntValue("retries", -1, 5, 1);
        loadPathsAndDriversFromHdd = getBooleanValue("hdd.load", false);
        csvStorageFolder = getPathValue("csv.storage.folder", "CSV_storage");
        maxResponseDelayMs = getIntValue("max.response.delay.ms", 10, 10000, 5000);
        maxSimulationTimeInSeconds = getIntValue("max.simulation.time.s", 0);
        if (maxSimulationTimeInSeconds > 0) {
            String pattern = Constants.getBundleValue("LimitedSimulationTime");
            maxSimulationTimeStringFormatted = MessageFormat.format(pattern, DurationFormatUtils.formatDuration(PresetSimulation.getMaxSimulationTimeMs(), "HH:mm:ss", true));
        } else {
            maxSimulationTimeStringFormatted = Constants.getBundleValue("Infinite");
        }
        statusSamplingIntervalInSeconds = getIntValue("status.sampling.interval.s", 1, 30, 1);
        consumerPollTimeout = getIntValue("consumer.poll.timeout.ms", 1000);

        loopingSimulation = getBooleanValue("looping.simulation", true);
        kafkaProducerPerSmartDriver = getBooleanValue("kafka.producer.per.driver", false);
    }

    private static String getPathValue(String propertyName, String defaultValue) {
        String property = PRESET_SIMULATION_PROPERTIES.getProperty(propertyName);

        if (!StorageUtils.canWrite(property)) {
            property = defaultValue;
            LOG.log(Level.SEVERE, "{0} property not declared or not writable, using default: {1}", new Object[]{propertyName, property});
        }
        return property;
    }

    private static int getIntValue(String propertyName, int defaultValue) {
        String property = PRESET_SIMULATION_PROPERTIES.getProperty(propertyName, String.valueOf(defaultValue));
        int value = defaultValue;

        try {
            value = Integer.parseInt(property);
        } catch (NumberFormatException ex) {
            LOG.log(Level.SEVERE, "validate() - Invalid value for {0}. Using default value: {1}", new Object[]{propertyName, defaultValue});
        }

        return value;
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

    //TODO
    public static Constants.SmartDrivers_Starting_Mode getStartingMode() {
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

    public static boolean isLoadPathsAndDriversFromHdd() {
        return loadPathsAndDriversFromHdd;
    }

    public static String getCsvStorageFolder() {
        return csvStorageFolder;
    }

    public static int getMaxResponseDelayMs() {
        return maxResponseDelayMs;
    }

    public static int getMaxSimulationTimeMs() {
        return maxSimulationTimeInSeconds * 1000;
    }

    public static String getMaxSimulationTimeStringFormatted() {
        return maxSimulationTimeStringFormatted;
    }

    public static int getStatusSamplingIntervalInSeconds() {
        return statusSamplingIntervalInSeconds;
    }

    public static int getConsumerPollTimeout() {
        return consumerPollTimeout;
    }

    public static boolean isLoopingSimulation() {
        return loopingSimulation;
    }

    public static boolean isKafkaProducerPerSmartDriver() {
        return kafkaProducerPerSmartDriver;
    }

    public static void setLoopingSimulation(boolean loopingSimulation) {
        PresetSimulation.loopingSimulation = loopingSimulation;
    }

    public static void setKafkaProducerPerSmartDriver(boolean kafkaProducerPerSmartDriver) {
        PresetSimulation.kafkaProducerPerSmartDriver = kafkaProducerPerSmartDriver;
    }
}
