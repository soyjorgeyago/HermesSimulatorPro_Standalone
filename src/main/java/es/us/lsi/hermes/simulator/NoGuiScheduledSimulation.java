package es.us.lsi.hermes.simulator;

import es.us.lsi.hermes.util.Constants;
import es.us.lsi.hermes.util.Util;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.util.Date;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.PostConstruct;
import javax.ejb.Singleton;
import javax.ejb.Startup;

@Singleton
@Startup
public class NoGuiScheduledSimulation {

    private static final Logger LOG = Logger.getLogger(NoGuiScheduledSimulation.class.getName());

    private Properties noGuiScheduledSimulationProperties;

    private static Integer distanceFromCenter;
    private static Integer maxPathDistance;
    private static Integer pathsAmount;
    private static Integer driversByPath;
    private static Integer pathsGenerationMethod;
    private static Integer streamServer;
    private static Integer startingMode;
    private static Boolean retryOnFail;
    private static Integer intervalBetweenRetriesInSeconds;
    private static Date scheduledSimulation;
    private static String sendResultsToEmail;
    private static Boolean randomizeEachSmartDriverBehaviour;
    private static Boolean monitorEachSmartDriver;
    private static Integer retries;

    @PostConstruct
    public void onStartup() {
        LOG.log(Level.INFO, "onStartup() - Se comprueba si existe un archivo de configuración de una simulación sin interfaz gráfica");

        try {
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            InputStream input = classLoader.getResourceAsStream("NoGuiScheduledSimulation.properties");
            noGuiScheduledSimulationProperties = new Properties();
            noGuiScheduledSimulationProperties.load(input);
            validate();
        } catch (IOException ex) {
            LOG.log(Level.INFO, "onStartup() - No existe un archivo de configuración válido (NoGuiScheduledSimulation.properties)");
        }
    }

    /**
     * Valida el archivo de propiedades completo. Si alguna propiedad no es
     * válida, se notifica por log. No se usará el archivo de propiedades si
     * algún valor es incorrecto.
     */
    private void validate() {

        String property;

        property = noGuiScheduledSimulationProperties.getProperty("distance.from.center");
        if (property != null) {
            distanceFromCenter = Integer.parseInt(property);
            if (distanceFromCenter < 1 || distanceFromCenter > 100) {
                distanceFromCenter = null;
                LOG.log(Level.SEVERE, "validate() - Valor no válido para 'distanceFromCenter' [1 a 100]");
            }
        }

        property = noGuiScheduledSimulationProperties.getProperty("max.path.distance");
        if (property != null) {
            maxPathDistance = Integer.parseInt(property);
            if (maxPathDistance < 1 || maxPathDistance > 100) {
                maxPathDistance = null;
                LOG.log(Level.SEVERE, "validate() - Valor no válido para 'maxPathDistance' [1 a 100]");
            }
        }

        property = noGuiScheduledSimulationProperties.getProperty("paths.amount");
        if (property != null) {
            pathsAmount = Integer.parseInt(property);
            if (pathsAmount < 1 || pathsAmount > 10) {
                pathsAmount = null;
                LOG.log(Level.SEVERE, "validate() - Valor no válido para 'pathsAmount' [1 a 10]");
            }
        }

        property = noGuiScheduledSimulationProperties.getProperty("drivers.by.path");
        if (property != null) {
            driversByPath = Integer.parseInt(property);
            if (driversByPath < 1 || driversByPath > 3000) {
                driversByPath = null;
                LOG.log(Level.SEVERE, "validate() - Valor no válido para 'driversByPath' [1 a 3000]");
            }
        }

        property = noGuiScheduledSimulationProperties.getProperty("paths.generation.method");
        if (property != null) {
            pathsGenerationMethod = Integer.parseInt(property);
            if (pathsGenerationMethod < 0 || pathsGenerationMethod > 1) {
                pathsGenerationMethod = null;
                LOG.log(Level.SEVERE, "validate() - Valor no válido para 'pathsGenerationMethod' [0 para Google o 1 para OpenStreetMap]");
            }
        }

        property = noGuiScheduledSimulationProperties.getProperty("stream.server");
        if (property != null) {
            streamServer = Integer.parseInt(property);
            if (streamServer < 0 || streamServer > 1) {
                streamServer = null;
                LOG.log(Level.SEVERE, "validate() - Valor no válido para 'streamServer' [0 para Kafka o 1 para Ztreamy]");
            }
        }

        property = noGuiScheduledSimulationProperties.getProperty("starting.mode");
        if (property != null) {
            startingMode = Integer.parseInt(property);
            if (startingMode < 0 || startingMode > 2) {
                startingMode = null;
                LOG.log(Level.SEVERE, "validate() - Valor no válido para 'startingMode' [0 para aleatorio, 1 para lineal o 2 para todos a la vez]");
            }
        }

        property = noGuiScheduledSimulationProperties.getProperty("retry.on.fail");
        if (property != null) {
            retryOnFail = Boolean.parseBoolean(property);
        }

        property = noGuiScheduledSimulationProperties.getProperty("interval.between.retries.s");
        if (property != null) {
            intervalBetweenRetriesInSeconds = Integer.parseInt(property);
            if (intervalBetweenRetriesInSeconds < 1 || intervalBetweenRetriesInSeconds > 60) {
                intervalBetweenRetriesInSeconds = null;
                LOG.log(Level.SEVERE, "validate() - Valor no válido para 'intervalBetweenRetriesInSeconds' [1 a 60]");
            }
        }

        property = noGuiScheduledSimulationProperties.getProperty("scheduled.simulation");
        if (property != null) {
            try {
                scheduledSimulation = Constants.dfFile.parse(property);
            } catch (ParseException ex) {
                scheduledSimulation = null;
                LOG.log(Level.SEVERE, "validate() - Formato de fecha no válida para 'scheduledSimulation' [yyyy-MM-dd_HH.mm.ss]");
            }
        }

        sendResultsToEmail = noGuiScheduledSimulationProperties.getProperty("send.results.to.email");
        if (!Util.isValidEmail(sendResultsToEmail)) {
            sendResultsToEmail = null;
            LOG.log(Level.SEVERE, "validate() - Valor no válido para 'sendResultsToEmail'. Debe indicarse un e-mail válido");
        }

        property = noGuiScheduledSimulationProperties.getProperty("randomize.behaviour");
        if (property != null) {
            randomizeEachSmartDriverBehaviour = Boolean.parseBoolean(property);
        }

        property = noGuiScheduledSimulationProperties.getProperty("monitor.each.driver");
        if (property != null) {
            monitorEachSmartDriver = Boolean.parseBoolean(property);
        }
        
        property = noGuiScheduledSimulationProperties.getProperty("retries");
        if (property != null) {
            retries = Integer.parseInt(property);
            if (retries < -1 || retries > 5) {
                retries = null;
                LOG.log(Level.SEVERE, "validate() - Valor no válido para 'retries' [-1 a 5]");
            }
        }
    }

    public static Integer getDistanceFromCenter() {
        return distanceFromCenter;
    }

    public static Integer getMaxPathDistance() {
        return maxPathDistance;
    }

    public static Integer getPathsAmount() {
        return pathsAmount;
    }

    public static Integer getDriversByPath() {
        return driversByPath;
    }

    public static Integer getPathsGenerationMethod() {
        return pathsGenerationMethod;
    }

    public static Integer getStreamServer() {
        return streamServer;
    }

    public static Integer getStartingMode() {
        return startingMode;
    }

    public static Boolean isRetryOnFail() {
        return retryOnFail;
    }

    public static Integer getIntervalBetweenRetriesInSeconds() {
        return intervalBetweenRetriesInSeconds;
    }

    public static Date getScheduledSimulation() {
        return scheduledSimulation;
    }

    public static String getSendResultsToEmail() {
        return sendResultsToEmail;
    }

    public static Boolean isRandomizeEachSmartDriverBehaviour() {
        return randomizeEachSmartDriverBehaviour;
    }

    public static Boolean isMonitorEachSmartDriver() {
        return monitorEachSmartDriver;
    }

    public static Integer getRetries() {
        return retries;
    }
}
