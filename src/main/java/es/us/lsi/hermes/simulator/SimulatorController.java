package es.us.lsi.hermes.simulator;

import com.google.gson.Gson;
import es.us.lsi.hermes.csv.ICSVBean;
import es.us.lsi.hermes.csv.SimulatorStatus;
import es.us.lsi.hermes.location.LocationLog;
import es.us.lsi.hermes.kafka.Kafka;
import es.us.lsi.hermes.location.detail.LocationLogDetail;
import es.us.lsi.hermes.util.*;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.mail.MessagingException;
import org.apache.commons.lang.time.DurationFormatUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class SimulatorController implements Serializable, ISimulatorControllerObserver {

    private static final Logger LOG = Logger.getLogger(SimulatorController.class.getName());

    private static List<LocationLog> locationLogList = new ArrayList<>();

    private static long startSimulationTime = 0L;
    private static long endSimulationTime = 0L;

    public enum State {
        CONFIG_CHANGED, READY_TO_SIMULATE, SCHEDULED_SIMULATION, SIMULATING, ENDED, INTERRUPTED
    }
    private static State currentState = State.READY_TO_SIMULATE;

    private static volatile SurroundingVehiclesConsumer surroundingVehiclesConsumer;
    private static ConcurrentHashMap<String, SimulatedSmartDriver> simulatedSmartDriverHashMap = new ConcurrentHashMap<>();

    private static ScheduledFuture emergencyScheduler;
    private static ScheduledFuture simulationScheduler;
    private static ScheduledFuture statusMonitorScheduler;
    private static String statusString;

    private static Date scheduledDate;

    private static ScheduledThreadPoolExecutor threadPool;

    private static int retries = 5;

    // Kafka
    private static AtomicLong kafkaRecordId;
    private static volatile KafkaProducer<Long, String> kafkaProducer;
    private static volatile KafkaProducer<String, String> kafkaMonitoringProducer;
    private static Properties kafkaProducerProperties;
    private static Properties kafkaMonitoringProducerProperties;
    private static boolean localMode;

    public SimulatorController(boolean localMode) {
        this.localMode = localMode;
        LOG.log(Level.INFO, "SimulatorController() - Simulator controller init. LOCAL MODE: {0}", localMode);

        // Attribute initialization.
        initAttributes();

        // Thread pool for simulated SmartDrivers initialization.
        initThreadPool();

        // Set the simulation configuration values from the PresetSimulation.properties files.
        initPresetSimulation();

        kafkaRecordId = new AtomicLong(0);
        kafkaProducerProperties = Kafka.getKafkaProducerProperties();
        kafkaMonitoringProducerProperties = Kafka.getKafkaMonitoringProducerProperties();
    }

    private void initAttributes() {
        simulatedSmartDriverHashMap = new ConcurrentHashMap<>();
    }

    private void initThreadPool() {
        threadPool = new ScheduledThreadPoolExecutor(100);
        threadPool.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        threadPool.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        threadPool.setRemoveOnCancelPolicy(true);
    }

    private void initPresetSimulation() {
        LOG.log(Level.INFO, "initPresetSimulation() - It will be loaded the configuration set in the 'PresetSimulation.properties' file.");
        LOG.log(Level.INFO, "initPresetSimulation() - Default rule: If the property is not set in the properties file or is not valid, it will be get the inner default behaviour.");

        // Use preset paths and drivers saved previously or get new ones.
        if (PresetSimulation.isLoadPathsAndDriversFromHdd()) {
            locationLogList = CSVUtils.loadPathsFromFolder();
        } else {
            PathUtils.generateSimulatedPaths();
        }

        // Initialize path related attributes to perform the simulation
        setupVariablesWithPaths(locationLogList.size());
    }

    private void setupVariablesWithPaths(int locationLogListSize) {
        LOG.log(Level.INFO, "generateSimulatedPaths() - Trayectos generados: {0}", locationLogListSize);
        currentState = State.READY_TO_SIMULATE;
        if (locationLogList.size() < PresetSimulation.getPathsAmount()) {
            if (PresetSimulation.getPathsAmount() > 0) {
                // Log the generated paths, although the user requested a higher amount.
                LOG.log(Level.SEVERE, "generateSimulatedPaths() - Only {0} paths could be generated", locationLogList.size());
            } else {
                LOG.log(Level.INFO, Constants.getBundleValue("UnableToGetPathsFromService"));
            }
        }
    }

    public void configChanged() {
        currentState = State.CONFIG_CHANGED;
    }

    public boolean isButtonStartStopEnabled() {
        return currentState.equals(State.READY_TO_SIMULATE) || currentState.equals(State.SCHEDULED_SIMULATION) || currentState.equals(State.SIMULATING);
    }

    private static void stopStatusMonitorTimer() {
        if (statusMonitorScheduler != null) {
            LOG.log(Level.INFO, "stopStatusMonitorTimer() - Stopping simulator status monitor.");
            statusMonitorScheduler.cancel(true);
            statusMonitorScheduler = null;
        }
    }

    private void startStatusMonitorTimer() {
        final String computerNameWithStartTime = Util.getComputerName() + "_" + System.currentTimeMillis();
        LOG.log(Level.INFO, "statusMonitorTimer() - Starting simulator status monitor on machine: {0}", computerNameWithStartTime);
        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        statusMonitorScheduler = scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                // Evaluate the mean delay.
                long totalDelaysMs = 0L;
                long leastElapsedTime = Long.MAX_VALUE;
                SimulatedSmartDriver mostRecentSmartDriver = null;
                ArrayList<SimulatedSmartDriver> pausedSimulatedSmartDrivers = new ArrayList<>();
                int generated = 0;
                int sent = 0;
                int oks = 0;
                int notOks = 0;
                int errors = 0;
                int recovered = 0;
                int pending = 0;
                long maxSmartDriversDelayMs = 0;
                long currentMeanSmartDriversDelayMs = -1;

                for (SimulatedSmartDriver ssd : simulatedSmartDriverHashMap.values()) {
                    if (!ssd.isPaused()) {
                        totalDelaysMs += ssd.getCurrentDelayMs();
                        if (ssd.getDriverSimulationTimeInSeconds() < leastElapsedTime) {
                            mostRecentSmartDriver = ssd;
                            leastElapsedTime = mostRecentSmartDriver.getDriverSimulationTimeInSeconds();
                        }
                    } else {
                        pausedSimulatedSmartDrivers.add(ssd);
                    }

                    // Global values.
                    generated += ssd.getGenerated();
                    sent += ssd.getSent();
                    oks += ssd.getOks();
                    notOks += ssd.getNotOks();
                    errors += ssd.getErrors();
                    recovered += ssd.getRecovered();
                    pending += ssd.getPending();

                }

                int activeSmartDrivers = threadPool.getQueue().size();
                if (activeSmartDrivers > 0) {
                    currentMeanSmartDriversDelayMs = (int) Math.ceil(totalDelaysMs / activeSmartDrivers);
                }
                statusString = MessageFormat.format("ESTADO: Tramas generadas={0}|Envíos realizados={1}|Oks={2}|NoOks={3}|Errores={4}|Recuperados={5}|No reenviados finalmente={6}|Hilos restantes={7}|Máximo retraso temporal total={8}ms|Retraso temporal actual={9}ms", generated, sent, oks, notOks, errors, recovered, pending, threadPool.getQueue().size(), maxSmartDriversDelayMs, currentMeanSmartDriversDelayMs);
                LOG.log(Level.FINE, "logCurrentStatus() - " + statusString);

                String json = new Gson().toJson(new SimulatorStatus(System.currentTimeMillis(), generated, sent, oks, notOks, errors, recovered, pending, activeSmartDrivers, currentMeanSmartDriversDelayMs, pausedSimulatedSmartDrivers.size()));
                LOG.log(Level.FINE, "statusMonitorTimer() - Simulation status JSON: {0}", json);
                kafkaMonitoringProducer.send(new ProducerRecord<>(Kafka.TOPIC_SIMULATOR_STATUS, computerNameWithStartTime, json));

                // If the current mean delay exceeds the threshold value, the most recent SmartDriver thread will be paused in order to improve the delay.
                if (currentMeanSmartDriversDelayMs > PresetSimulation.getMaxResponseDelayMs()) {
                    if (mostRecentSmartDriver != null) {
                        try {
                            mostRecentSmartDriver.pauseSmartDriver();
                        } catch (InterruptedException ex) {
                            LOG.log(Level.SEVERE, "statusMonitorTimer() - Can't pause SimulatedSmartDriver {0}", mostRecentSmartDriver.getSha());
                        }
                    }
                } else {
                    // If there are paused SmartDrivers and the current mean delay is below the threshold value, it will resume one SmartDriver at a time.
                    if (!pausedSimulatedSmartDrivers.isEmpty()) {
                        try {
                            pausedSimulatedSmartDrivers.get(0).resumeSmartDriver();
                        } catch (InterruptedException ex) {
                            LOG.log(Level.SEVERE, "statusMonitorTimer() - Can't resume SimulatedSmartDriver {0}", pausedSimulatedSmartDrivers.get(0).getSha());
                        }
                    }
                }
            }
        }, 1, PresetSimulation.getStatusSamplingIntervalInSeconds(), TimeUnit.SECONDS
        );
    }

    public void simulate() {
        if (scheduledDate != null) {
            scheduledSimulation();
        } else {
            executeSimulation();
        }
    }

    private void scheduledSimulation() {
        // Es una simulación programada.
        LOG.log(Level.INFO, "scheduledSimulation() - Simulación programada: {0}", Constants.dfISO8601.format(scheduledDate));
        long delay = scheduledDate.getTime() - System.currentTimeMillis();
        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        simulationScheduler = scheduledExecutorService.schedule(new Runnable() {
            @Override
            public void run() {
                executeSimulation();
            }
        }, delay, TimeUnit.MILLISECONDS
        );
        currentState = State.SCHEDULED_SIMULATION;
    }

    private static void cancelScheduledSimulation() {
        if (simulationScheduler != null) {
            simulationScheduler.cancel(true);
        }
    }

    private void executeSimulation() {
        currentState = State.SIMULATING;

        kafkaProducer = new KafkaProducer<>(kafkaProducerProperties);
        kafkaMonitoringProducer = new KafkaProducer<>(kafkaMonitoringProducerProperties);

        startSimulationTime = System.currentTimeMillis();
        LOG.log(Level.INFO, "executeSimulation() - Comienzo de la simulación: {0}", Constants.dfISO8601.format(startSimulationTime));
        LOG.log(Level.INFO, "executeSimulation() - Se inicia el consumidor de análisis de vehículos cercanos");

        if (!PresetSimulation.isKafkaProducerPerSmartDriver()) {
            surroundingVehiclesConsumer = new SurroundingVehiclesConsumer(this);
            surroundingVehiclesConsumer.start();
        }

        // Simulator status monitor initCSV.
        startStatusMonitorTimer();

        boolean pathNumberWarnings = PresetSimulation.getPathsAmount() != locationLogList.size();

        // Creation of simulated Smart Drivers.
        String simulationSummary = MessageFormat.format(
                "\n-> ¿Retry failed messages?: {2}"
                + "\n-> Seconds between retries: {3}"
                + "\n-> SmartDrivers start mode: {4}"
                + "\n-> Paths requested: {6}" + (pathNumberWarnings ? " Path generated {7} - WARNING" : "")
                + "\n-> Drivers per paths requested: {8}"
                + "\n-> Number of threads that will be created: {5}"
                + "\n-> Maximum simulation time: {9}"
                + "\n-> Paths and Drivers from disk: {10}",
                PresetSimulation.isRetryOnFail(),
                PresetSimulation.getIntervalBetweenRetriesInSeconds(),
                PresetSimulation.getStartingMode().name(),
                PresetSimulation.getDriversByPath() * locationLogList.size(),
                PresetSimulation.getPathsAmount(),
                locationLogList.size(),
                PresetSimulation.getDriversByPath(),
                PresetSimulation.getMaxSimulationTimeStringFormatted(),
                PresetSimulation.isLoadPathsAndDriversFromHdd());
        LOG.log(Level.INFO, "SimulatorController - executeSimulation() - FINAL CONDITIONS: {0}", simulationSummary);

        List<List<DriverParameters>> loadedDriverParameters = null;
        if (PresetSimulation.isLoadPathsAndDriversFromHdd()) {
            loadedDriverParameters = CSVUtils.loadSimulatedDriversForPath();
        }

        try {
            long id = 0L;
            for (int i = 0; i < PresetSimulation.getPathsAmount(); i++) {
                LocationLog ll = locationLogList.get(i);

                // Para el caso del modo de inicio LINEAL, si hay más de 10 SmartDrivers, se toma el 10% para repartir su inicio durante 50 segundos.
                int smartDriversBunch = PresetSimulation.getDriversByPath() > 10 ? (int) (PresetSimulation.getDriversByPath() * 0.10) : 1;

                LOG.log(Level.FINE, "executeSimulation() - Cada 10 segundos, se iniciarán {0} SmartDrivers en el trayecto {1}", new Object[]{smartDriversBunch, i});

                List<ICSVBean> driverParameters = new ArrayList<>();
                for (int j = 0; j < PresetSimulation.getDriversByPath(); j++) {

                    DriverParameters dp;
                    if (PresetSimulation.isLoadPathsAndDriversFromHdd() && loadedDriverParameters != null) {
                        dp = loadedDriverParameters.get(i).get(j);
                    } else {
                        if (PresetSimulation.isRandomizeEachSmartDriverBehaviour()) {
                            // Aleatory values.
                            dp = new DriverParameters();
                        } else {
                            // Same preset values.
                            dp = new DriverParameters(1, 1);
                        }
                    }

                    initSimulatedSmartDriver(id, i, dp, smartDriversBunch, dp.getSpeedRandomFactor(), dp.getHrRandomFactor());
                    driverParameters.add(dp);
                    id++;
                }

                if (!PresetSimulation.isLoadPathsAndDriversFromHdd()) {
                    CSVUtils.createDriversDataFile(String.valueOf(i + 1), driverParameters);
                }
            }

            if (PresetSimulation.getMaxSimulationTimeMs() > 0) {
                LOG.log(Level.INFO, "simulate() {0}", PresetSimulation.getMaxSimulationTimeStringFormatted());
                startShutdownTimer();
            }
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, "executeSimulation() - Ha ocurrido un problema al crear los hilos de ejecución. Se cancela la simulación", ex);
            // Cancelamos las simulaciones.
            finishSimulation(true);
        }
    }

    private void initSimulatedSmartDriver(long id, int pathId, DriverParameters dp, int smartDriversBunch, double speedRandomFactor, double hrRandomFactor) throws MalformedURLException, HermesException {

        SimulatedSmartDriver ssd = new SimulatedSmartDriver(id, pathId, dp, PresetSimulation.isLoopingSimulation(), retries, speedRandomFactor, hrRandomFactor);
        simulatedSmartDriverHashMap.put(ssd.getSha(), ssd);

        long delay = 0L;
        // Aplicamos el modo de inicio seleccionado a los SmartDrivers que creamos.
        switch (PresetSimulation.getStartingMode()) {
            case ALEATORY:
                Random rand = new Random();
                delay = rand.nextInt(Constants.MAX_INITIAL_DELAY);
                break;
            case LINEAL:
                // Se repartirá el total de SmartDrivers en 50 segundos.
                // Para cada trayecto, saldrá el 10% de SmartDrivers cada 10 segundos, con lo que el total habrá salido antes de 100 segundos.
                delay = 10000 * (int) (id / smartDriversBunch);
                break;
            case SAME_TIME:
                break;
        }
        // Aplicamos un pequeño retraso más el aplicado por el modo se inicio.
        long totalDelay = 100 + id + delay;
        LOG.log(Level.FINE, "SmartDriver {0} con inicio en {1}", new Object[]{id, totalDelay});
        threadPool.scheduleAtFixedRate(ssd, totalDelay, 1000, TimeUnit.MILLISECONDS);
    }

    private void resetSimulation() {

        LOG.log(Level.INFO, "resetSimulation() - Se envía señal de cierre a las conexiones con el servidor de streams");
        // Cerramos todas las conexiones de envío de datos, ya sea de Ztreamy o de Kafka, si las hubiera.
        if (!simulatedSmartDriverHashMap.isEmpty()) {
            for (SimulatedSmartDriver ssd : simulatedSmartDriverHashMap.values()) {
                ssd.finish();
            }
        }

        // Finalizamos todos los hilos de los SmartDrivers, si los hubiera.
        if (threadPool != null) {
            LOG.log(Level.INFO, "resetSimulation() - Se envía señal de finalización a los hilos");
            threadPool.shutdownNow();
        }

        initAttributes();

        initThreadPool();
    }

    public static void smartDriverHasFinished(String id) {
        LOG.log(Level.FINE, "smartDriverHasFinished() - Ha terminado el SmartDriver con id={0}, quedan {1} restantes", new Object[]{id, threadPool.getQueue().size()});
        simulatedSmartDriverHashMap.remove(id);
    }

    private synchronized void finishSimulation(boolean interrupted) {
        try {
            cancelScheduledSimulation();
            stopShutdownTimer();
            stopStatusMonitorTimer();

            if (currentState.equals(State.SIMULATING)) {
                if (interrupted) {
                    LOG.log(Level.SEVERE, "finishSimulation() - Se fuerza el fin de la simulación. Duración: {0}", DurationFormatUtils.formatDuration((System.currentTimeMillis() - startSimulationTime), "HH:mm:ss", true));
                    LOG.log(Level.SEVERE, "finishSimulation() - ########## SIMULACION INTERRUMPIDA ##########");
                }
                LOG.log(Level.INFO, "finishSimulation() - Se para el consumidor de análisis de vehículos cercanos");

                if (surroundingVehiclesConsumer != null) {
                    surroundingVehiclesConsumer.stopConsumer();
                }
                endSimulationTime = System.currentTimeMillis();
                String timeSummary = MessageFormat.format("Inicio de la simulacion: {0} -> Fin de la simulación: {1} ({2})", Constants.dfISO8601.format(startSimulationTime), Constants.dfISO8601.format(endSimulationTime), DurationFormatUtils.formatDuration(endSimulationTime - startSimulationTime, "HH:mm:ss", true));
                LOG.log(Level.INFO, "finishSimulation() - {0}", timeSummary);

                String body = "<html><head><title></title></head><body>" + (interrupted ? "<h1 style=\"color:red;\">SIMULACION INTERRUMPIDA</h1>" : "") + "<p>" + statusString.replaceAll("\n", "<br/>") + "</p><p>" + timeSummary + "</p><p>Un saludo.</p></body></html>";
                Email.generateAndSendEmail(PresetSimulation.getSendResultsToEmail(), "FIN DE SIMULACION " + Util.getComputerName(), body);
            }
        } catch (MessagingException ex) {
            LOG.log(Level.SEVERE, "finishSimulation() - No se ha podido enviar el e-mail con los resultados de la simulación", ex.getCause());
        } finally {
            currentState = interrupted ? State.INTERRUPTED : State.ENDED;

            resetSimulation();

            // JYFR: PRUEBA
//            // Comprobamos si hay que ejecutar otra simulación.
//            if (streamServer.equals(Stream_Server.FIRST_KAFKA_THEN_ZTREAMY) || streamServer.equals(Stream_Server.FIRST_ZTREAMY_THEN_KAFKA)) {
//                // Planificamos la fecha de inicio para después del tiempo máximo de simulación más 5 minutos para tener margen.
//                // De este modo, si se planificó una simulación a una hora determinada en varios equipos, todos empezarán la siguiente al mismo tiempo también.
//                scheduledDate = new Date(startSimulationTime + MAX_SIMULATION_TIME + 300000);
//                streamServer = Stream_Server.values()[(streamServer.ordinal() - 1) % 2];
//                LOG.log(Level.INFO, "finishSimulation() - La siguiente simulación será a las: {0}", Constants.dfISO8601.format(scheduledDate));
//                scheduledSimulation();
//            }
            if (kafkaProducer != null) {
                kafkaProducer.flush();
                kafkaProducer.close();
                // FIXME: Algunas veces salta una excepción de tipo 'java.lang.InterruptedException'.
                // Es un 'bug' que aún está en estado aabierto en Kafka.
                // https://issues.streamsets.com/browse/SDC-4925
            }

            if (kafkaMonitoringProducer != null) {
                kafkaMonitoringProducer.flush();
                kafkaMonitoringProducer.close();
                // FIXME: Algunas veces salta una excepción de tipo 'java.lang.InterruptedException'.
                // Es un 'bug' que aún está en estado aabierto en Kafka.
                // https://issues.streamsets.com/browse/SDC-4925
            }
        }
    }

    public static boolean isConfigLock() {
        switch (currentState) {
            case SIMULATING:
            case SCHEDULED_SIMULATION:
            case ENDED:
            case INTERRUPTED:
                return true;
            default:
                return false;
        }
    }

    public static boolean checkFinished() {
        return currentState.equals(State.READY_TO_SIMULATE);
    }

    @Override
    public void update(String id, int surroundingSize) {
        SimulatedSmartDriver ssd = simulatedSmartDriverHashMap.get(id);
        if (ssd != null) {
            ssd.stressBySurrounding(surroundingSize);
        }
    }

    public Date getScheduledDate() {
        return scheduledDate;
    }

    public void setScheduledDate(Date sd) {
        scheduledDate = sd;
    }

    public void resetScheduledDate() {
        scheduledDate = null;
    }

    private static void stopShutdownTimer() {
        if (emergencyScheduler != null) {
            emergencyScheduler.cancel(true);
        }
    }

    private void startShutdownTimer() {
        if (emergencyScheduler != null) {
            emergencyScheduler.cancel(true);
        }
        // Shutdown timer to set a limited simulation time. Once it reaches the set duration, it will finish the execution.
        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        emergencyScheduler = scheduledExecutorService.scheduleAtFixedRate(new EmergencyShutdown(startSimulationTime, PresetSimulation.getMaxSimulationTimeMs() + 60000), 0, 5, TimeUnit.SECONDS);
    }

    public static boolean isKafkaProducerPerSmartDriver() {
        return PresetSimulation.isKafkaProducerPerSmartDriver();
    }

    public int getRetries() {
        return retries;
    }

    public void setRetries(int r) {
        retries = r;
    }

    public static long getNextKafkaRecordId() {
        return kafkaRecordId.getAndIncrement();
    }

    public static synchronized KafkaProducer<Long, String> getKafkaProducer() {
        return kafkaProducer;
    }

    public static synchronized List<LocationLog> getLocationLogList() {
        return locationLogList;
    }

    public static long getStartSimulationTime() {
        return startSimulationTime;
    }

    public static boolean isLocalMode() {
        return localMode;
    }

    public static List<LocationLogDetail> getPath(int pathId) {
        try {
            return locationLogList.get(pathId).getLocationLogDetailList();
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, "getPath() - Error getting path {0}", pathId);
        }

        return new ArrayList();
    }

    class EmergencyShutdown implements Runnable {

        // Marca temporal del inicio de la simulación.
        private final long startSimulationTime;

        // Tiempo máximo de simulación.
        private final long duration;

        public EmergencyShutdown(long start, long duration) {
            this.startSimulationTime = start;
            this.duration = duration;
        }

        @Override
        public void run() {
            // Si ha pasado más tiempo que el establecido en los parámetros de simulación, forzamos la cancelación.
            if ((System.currentTimeMillis() - startSimulationTime) >= duration) {
                LOG.log(Level.SEVERE, "finishSimulation() - ########## SIMULACION INTERRUMPIDA POR SOBREPASAR EL TIEMPO MÁXIMO ##########");
                finishSimulation(true);
            }
        }
    }
}
