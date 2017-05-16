package es.us.lsi.hermes.simulator;

import com.google.gson.Gson;
import com.google.maps.model.LatLng;
import es.us.lsi.hermes.analysis.Vehicle;
import es.us.lsi.hermes.csv.SimulatorStatus;
import es.us.lsi.hermes.location.detail.LocationLogDetail;
import es.us.lsi.hermes.location.LocationLog;
import es.us.lsi.hermes.kafka.Kafka;
import es.us.lsi.hermes.util.*;
import static es.us.lsi.hermes.util.Util.getComputerName;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.nio.file.Path;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.mail.MessagingException;
import org.apache.commons.lang.time.DurationFormatUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class SimulatorController implements Serializable, ISimulatorControllerObserver {

    private static final Logger LOG = Logger.getLogger(SimulatorController.class.getName());

    // Número de tramas de Ztreamy generadas.
    private static final AtomicInteger GENERATED = new AtomicInteger(0);
    // Número de errores contabilizados al enviar las tramas a Ztreamy, distintos de los 'no OK'.
    private static final AtomicInteger ERRORS = new AtomicInteger(0);
    // Número de tramas enviadas a Ztreamy correctamente.
    private static final AtomicInteger OK = new AtomicInteger(0);
    // Número de tramas enviadas a Ztreamy con recepción de 'no OK'.
    private static final AtomicInteger NOT_OK = new AtomicInteger(0);
    // Número de tramas enviadas a Ztreamy con recepción de 'no OK' o erróneas, que se han podido reenviar.
    private static final AtomicInteger RECOVERED = new AtomicInteger(0);
    // Número de tramas enviadas a Ztreamy que no se han podido reenviar porque ha terminado la simulación de cada trayecto.
    private static final AtomicInteger FINALLY_PENDING = new AtomicInteger(0);
    // Número de envíos que se han realizado, ya sean correctos o fallidos.
    private static final AtomicInteger SENT = new AtomicInteger(0);

    // Ratio de ejecución para la simulación.
    private enum Time_Rate {
        X1(1000), X10(100), X100(10), X1000(1);
        private final int milliseconds;

        Time_Rate(int ms) {
            this.milliseconds = ms;
        }

        public int getMilliseconds() {
            return milliseconds;
        }
    }
    // Por defecto será en tiempo real.
    private static Time_Rate timeRate = Time_Rate.X1;

    private static Constants.Paths_Generation_Method pathsGenerationMethod;

    // Los trayectos generados puede que no tengan la densidad de puntos necesaria para tener una posición en cada segundo de la simulación.
    // Además, cada 'SmartDriver' tendrá sus características de conducción, con lo que si fuera más rápido harían falta menos puntos y si fuera más lento
    // harían falta más puntos. Se calculará la interpolación tomando la velocidad mínima de 10Km/h.
    // FIXME: Transfer to properties file.
    private static boolean interpolate = true;

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

    // Maximum delay among all SmartDrivers.
    private static AtomicLong maxSmartDriversDelayMs;
    // Current mean delay of all SmartDrivers.
    private static AtomicLong currentMeanSmartDriversDelayMs;

    // Directorio temporal para almacenar los archivos generados.
    // TODO: Still necessary?
    private static Path tempFolder;

    private enum Stream_Server {
        KAFKA, ZTREAMY, FIRST_KAFKA_THEN_ZTREAMY, FIRST_ZTREAMY_THEN_KAFKA
    }
    private static Stream_Server streamServer = Stream_Server.KAFKA;

    private static Date scheduledDate;

    // Mecanismos de inicio de los conductores.
    public enum SmartDrivers_Starting_Mode {
        ALEATORY, LINEAL, SAME_TIME
    }

    // Por defecto, establecemos que el inicio de los usuarios siga una progresión lineal.
    private static SmartDrivers_Starting_Mode startingMode = SmartDrivers_Starting_Mode.LINEAL;

    private static ScheduledThreadPoolExecutor threadPool;

    private static boolean randomizeEachSmartDriverBehaviour = true;

    private static int retries = 5;

    // TODO: Relocate in PresetSimulation.properties
    private static boolean loopingSimulation = true;
    static boolean kafkaProducerPerSmartDriver = true;

    // Kafka
    private static AtomicLong kafkaRecordId;
    private static volatile KafkaProducer<Long, String> kafkaProducer;
    private static volatile KafkaProducer<String, String> kafkaMonitorigProducer;
    private static Properties kafkaProducerProperties;
    private static Properties kafkaMonitoringProducerProperties;

    public SimulatorController() {
        LOG.log(Level.INFO, "SimulatorController() - Simulator controller init.");

        // Attribute initialization.
        initAttributes();

        // Thread pool for simulated SmartDrivers initialization.
        initThreadPool();

        maxSmartDriversDelayMs = new AtomicLong(0);
        currentMeanSmartDriversDelayMs = new AtomicLong(0);

        // Set the simulation configuration values from the PresetSimulation.properties files.
        initPresetSimulation();

        kafkaRecordId = new AtomicLong(0);
        kafkaProducerProperties = Kafka.getKafkaProducerProperties();
        kafkaMonitoringProducerProperties = Kafka.getKafkaMonitoringProducerProperties();
    }

    private void initAttributes() {
        simulatedSmartDriverHashMap = new ConcurrentHashMap<>();
        GENERATED.set(0);
        OK.set(0);
        NOT_OK.set(0);
        RECOVERED.set(0);
        ERRORS.set(0);
        FINALLY_PENDING.set(0);
        SENT.set(0);
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

        // Default rule: If the property is not set in the properties file or is not valid, it will be get the inner default behaviour.
        // Tiene que haber una coherencia entre trayectos y conductores, para no saturar el sistema.
        setPathsGenerationMethod(PresetSimulation.getPathsGenerationMethod());

        // Use pre-calculated routes or get new ones if requested
        if (PresetSimulation.isUseRoutesFromHdd()) {
            locationLogList = CSVUtils.extractSimulatedPaths();
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

    public void setPathsGenerationMethod(int value) {
        try {
            pathsGenerationMethod = Constants.Paths_Generation_Method.values()[value];
        } catch (Exception ex) {
            // Si no fuera un valor válido, establecemos un valor por defecto.
            pathsGenerationMethod = Constants.Paths_Generation_Method.GOOGLE;
        }
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
        LOG.log(Level.INFO, "statusMonitorTimer() - Starting simulator status monitor.");
        final String computerNameWithStartTime = Util.getComputerName() + "_" + System.currentTimeMillis();
        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        statusMonitorScheduler = scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                // Evaluate the mean delay.
                long totalDelaysMs = 0L;
                for (SimulatedSmartDriver ssd : simulatedSmartDriverHashMap.values()) {
                    totalDelaysMs += ssd.getCurrentDelayMs();
                }

                currentMeanSmartDriversDelayMs.set(totalDelaysMs / simulatedSmartDriverHashMap.size());
                logCurrentStatus();

                String json = new Gson().toJson(new SimulatorStatus(System.currentTimeMillis(), GENERATED.intValue(), SENT.intValue(), OK.intValue(), NOT_OK.intValue(), ERRORS.intValue(), RECOVERED.intValue(), FINALLY_PENDING.intValue(), threadPool.getQueue().size(), currentMeanSmartDriversDelayMs.get()));
                LOG.log(Level.FINE, "statusMonitorTimer() - Simulation status JSON: {0}", json);
                SimulatorController.getKafkaMonitoringProducer().send(new ProducerRecord<>(Kafka.TOPIC_SIMULATOR_STATUS, computerNameWithStartTime, json));
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
        kafkaMonitorigProducer = new KafkaProducer<>(kafkaMonitoringProducerProperties);

        tempFolder = StorageUtils.createTempFolder();
        startSimulationTime = System.currentTimeMillis();
        LOG.log(Level.INFO, "executeSimulation() - Comienzo de la simulación: {0}", Constants.dfISO8601.format(startSimulationTime));
        LOG.log(Level.INFO, "executeSimulation() - Envío de tramas a: {0}", Stream_Server.values()[streamServer.ordinal() % 2].name());
        LOG.log(Level.INFO, "executeSimulation() - Se inicia el consumidor de análisis de vehículos cercanos");
        surroundingVehiclesConsumer = new SurroundingVehiclesConsumer(this);
        surroundingVehiclesConsumer.start();

        // Simulator status monitor init.
        startStatusMonitorTimer();

        boolean pathNumberWarnings = PresetSimulation.getPathsAmount() != locationLogList.size();

        // Creation of simulated Smart Drivers.
        String simulationSummary = MessageFormat.format(
                "\n-> Simulation speed: {0}."
                + "\n-> Real time execution: {1}"
                + "\n-> ¿Retry failed messages?: {2}"
                + "\n-> Seconds between retries: {3}"
                + "\n-> SmartDrivers start mode: {4}"
                + "\n-> Paths requested: {6}" + (pathNumberWarnings ? " Path generated {7} - WARNING" : "")
                + "\n-> Drivers per paths requested: {8}"
                + "\n-> Number of threads that will be created: {5}"
                + "\n-> Maximum simulation time: {9}",
                timeRate.name(),
                timeRate.equals(Time_Rate.X1),
                PresetSimulation.isRetryOnFail(),
                PresetSimulation.getIntervalBetweenRetriesInSeconds(),
                startingMode.name(),
                PresetSimulation.getDriversByPath() * locationLogList.size(),
                PresetSimulation.getPathsAmount(),
                locationLogList.size(),
                PresetSimulation.getDriversByPath(),
                PresetSimulation.getMaxSimulationTimeStringFormatted());
        LOG.log(Level.INFO, "SimulatorController - executeSimulation() - FINAL CONDITIONS: {0}", simulationSummary);

        try {
            long id = 0L;
            for (int i = 0; i < locationLogList.size(); i++) {
                LocationLog ll = locationLogList.get(i);
                LocationLogDetail smartDriverPosition = (LocationLogDetail) ll.getLocationLogDetailList().get(0);
                LatLng latLng = new LatLng(smartDriverPosition.getLatitude(), smartDriverPosition.getLongitude());

                // Para el caso del modo de inicio LINEAL, si hay más de 10 SmartDrivers, se toma el 10% para repartir su inicio durante 50 segundos.
                int smartDriversBunch = PresetSimulation.getDriversByPath() > 10 ? (int) (PresetSimulation.getDriversByPath() * 0.10) : 1;

                LOG.log(Level.FINE, "executeSimulation() - Cada 10 segundos, se iniciarán {0} SmartDrivers en el trayecto {1}", new Object[]{smartDriversBunch, i});
                initSimulatedSmartDriver(id, ll, latLng, smartDriversBunch);
                id++;

                for (int j = 1; j < PresetSimulation.getDriversByPath(); j++) {
                    initSimulatedSmartDriver(id, ll, latLng, smartDriversBunch);
                    id++;
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

    private void initSimulatedSmartDriver(long id, LocationLog ll, LatLng latLng, int smartDriversBunch) throws MalformedURLException, HermesException {

        SimulatedSmartDriver ssd = new SimulatedSmartDriver(id, ll, randomizeEachSmartDriverBehaviour, loopingSimulation, streamServer.ordinal() % 2, retries);
        simulatedSmartDriverHashMap.put(ssd.getSha(), ssd);

        long delay = 0;
        // Aplicamos el modo de inicio seleccionado a los SmartDrivers que creamos.
        switch (startingMode) {
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
                delay = 0;
                break;
            default:
                // Por defecto se establecerá un inicio aleatorio.
                startingMode = SmartDrivers_Starting_Mode.ALEATORY;
                break;
        }
        // Aplicamos un pequeño retraso más el aplicado por el modo se inicio.
        long totalDelay = 100 + id + delay;
        LOG.log(Level.FINE, "SmartDriver {0} con inicio en {1}", new Object[]{id, totalDelay});
        threadPool.scheduleAtFixedRate(ssd, totalDelay, timeRate.getMilliseconds(), TimeUnit.MILLISECONDS);
        // TODO: Consumer
//                        ssd.startConsumer();
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

    public int getSimulatedSpeed() {
        return timeRate.ordinal();
    }

    public void setSimulatedSpeed(int tr) {
        try {
            timeRate = Time_Rate.values()[tr];
        } catch (Exception ex) {
            // Si no fuera un valor válido, establecemos un valor por defecto.
            timeRate = Time_Rate.X1;
        }
    }

    public static void smartDriverHasFinished(String id) {
        LOG.log(Level.FINE, "smartDriverHasFinished() - Ha terminado el SmartDriver con id={0}, quedan {1} restantes", new Object[]{id, threadPool.getQueue().size()});
        SimulatedSmartDriver ssd = simulatedSmartDriverHashMap.remove(id);
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
                surroundingVehiclesConsumer.stopConsumer();
                String simulationSummary;
                if (interrupted || ERRORS.get() > 0 || NOT_OK.get() > 0) {
                    simulationSummary = MessageFormat.format("RESULTADO DE LA SIMULACION:\n\n-> Servidor de tramas={0}\n\n-> Tramas generadas={1}\n-> Envíos realizados={2}\n-> Oks={3}\n-> NoOks={4}\n-> Errores={5}\n-> Recuperados={6}\n-> No reenviados finalmente={7}\n-> Hilos restantes={8}\n-> Trayectos={9}\n-> Distancia máxima={10}\n-> Instancias SmartDriver por trayecto={11}\n-> Reintentar fallidos={12}\n-> Segundos entre reintentos={13}\n-> Máximo retraso temporal={14}s\n\n", Stream_Server.values()[streamServer.ordinal() % 2].name(), GENERATED, SENT, OK, NOT_OK, ERRORS, RECOVERED, FINALLY_PENDING, threadPool.getQueue().size(), locationLogList.size(), PresetSimulation.getMaxPathDistance(), PresetSimulation.getDriversByPath(), PresetSimulation.isRetryOnFail(), PresetSimulation.getIntervalBetweenRetriesInSeconds(), Constants.df2Decimals.format(maxSmartDriversDelayMs.get() / 1000.0d));
                    LOG.log(Level.SEVERE, "finishSimulation() - {0}", simulationSummary);
                } else {
                    simulationSummary = MessageFormat.format("RESULTADO DE LA SIMULACION:\n\nLos envíos se han realizado correctamente:\n\n-> Servidor de tramas={0}\n\n-> Tramas generadas={1}\n-> Oks={2}\n-> Hilos restantes={3}\n-> Trayectos={4}\n-> Distancia máxima={5}\n-> Instancias SmartDriver por trayecto={6}\n-> Reintentar fallidos={7}\n-> Segundos entre reintentos={8}\n-> Máximo retraso temporal={9}s\n\n", Stream_Server.values()[streamServer.ordinal() % 2].name(), GENERATED, OK, threadPool.getQueue().size(), locationLogList.size(), PresetSimulation.getMaxPathDistance(), PresetSimulation.getDriversByPath(), PresetSimulation.isRetryOnFail(), PresetSimulation.getIntervalBetweenRetriesInSeconds(), Constants.df2Decimals.format(maxSmartDriversDelayMs.get() / 1000.0d));
                    LOG.log(Level.INFO, "finishSimulation() - {0}", simulationSummary);
                }

                endSimulationTime = System.currentTimeMillis();
                String timeSummary = MessageFormat.format("Inicio de la simulacion: {0} -> Fin de la simulación: {1} ({2})", Constants.dfISO8601.format(startSimulationTime), Constants.dfISO8601.format(endSimulationTime), DurationFormatUtils.formatDuration(endSimulationTime - startSimulationTime, "HH:mm:ss", true));
                LOG.log(Level.INFO, "finishSimulation() - {0}", timeSummary);

                int i = 1;
                String body = "<html><head><title></title></head><body>" + (interrupted ? "<h1 style=\"color:red;\">SIMULACION INTERRUMPIDA</h1>" : "") + "<p>" + simulationSummary.replaceAll("\n", "<br/>") + "</p><p>" + timeSummary + "</p><p>Un saludo.</p></body></html>";
                Email.generateAndSendEmail(PresetSimulation.getSendResultsToEmail(), "FIN DE SIMULACION " + getComputerName(), body);
            }
        } catch (MessagingException ex) {
            LOG.log(Level.SEVERE, "finishSimulation() - No se ha podido enviar el e-mail con los resultados de la simulación", ex.getCause());
        } finally {
            if (interrupted) {
                currentState = State.INTERRUPTED;
            } else {
                currentState = State.ENDED;
            }
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

            if (kafkaMonitorigProducer != null) {
                kafkaMonitorigProducer.flush();
                kafkaMonitorigProducer.close();
                // FIXME: Algunas veces salta una excepción de tipo 'java.lang.InterruptedException'.
                // Es un 'bug' que aún está en estado aabierto en Kafka.
                // https://issues.streamsets.com/browse/SDC-4925
            }
        }
    }

    public static void increaseGenerated() {
        GENERATED.incrementAndGet();
    }

    public static void increaseOkSends() {
        OK.incrementAndGet();
    }

    public static void increaseNoOkSends() {
        NOT_OK.incrementAndGet();
    }

    public static void addRecovered(int recovered) {
        RECOVERED.addAndGet(recovered);
    }

    public static void increaseErrors() {
        ERRORS.incrementAndGet();
    }

    public static void addFinallyPending(int pending) {
        FINALLY_PENDING.addAndGet(pending);
    }

    public static void increaseSends() {
        SENT.incrementAndGet();
    }

    private void logCurrentStatus() {
        LOG.log(Level.FINE, "logCurrentStatus() - ESTADO ACTUAL: Tramas generadas={0}|Envíos realizados={1}|Oks={2}|NoOks={3}|Errores={4}|Recuperados={5}|No reenviados finalmente={6}|Hilos restantes={7}|Máximo retraso temporal total={8}ms|Retraso temporal actual={9}ms", new Object[]{GENERATED.get(), SENT.get(), OK.get(), NOT_OK.get(), ERRORS.get(), RECOVERED.get(), FINALLY_PENDING.get(), threadPool.getQueue().size(), maxSmartDriversDelayMs.get(), currentMeanSmartDriversDelayMs.get()});
    }

    public static boolean isInterpolate() {
        return interpolate;
    }

    public static Path getTempFolder() {
        return tempFolder;
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

    public int getStreamServer() {
        return streamServer.ordinal();
    }

    public void setStreamServer(int value) {
        try {
            streamServer = Stream_Server.values()[value];
            if (streamServer.ordinal() > 1) {
                loopingSimulation = false;
            }
            if (streamServer.ordinal() % 2 != 0) {
                kafkaProducerPerSmartDriver = false;
            }
        } catch (Exception ex) {
            // Si no fuera un valor válido, establecemos un valor por defecto.
            streamServer = Stream_Server.KAFKA;
        }
    }

    public static boolean checkFinished() {
        return currentState.equals(State.READY_TO_SIMULATE);
    }

    @Override
    public void update(Vehicle v) {
        SimulatedSmartDriver ssd = simulatedSmartDriverHashMap.get(v.getId());
        if (ssd != null) {
            if (!v.getSurroundingVehicles().isEmpty()) {
                // TODO
            } else {
                // TODO
            }
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

    public int getStartingMode() {
        return startingMode.ordinal();
    }

    public void setStartingMode(int sm) {
        try {
            startingMode = SmartDrivers_Starting_Mode.values()[sm];
        } catch (Exception ex) {
            // Si no fuera un valor válido, establecemos un valor por defecto.
            startingMode = SmartDrivers_Starting_Mode.ALEATORY;
        }
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

    public boolean isLoopingSimulation() {
        return loopingSimulation;
    }

    public boolean isKafkaProducerPerSmartDriver() {
        return kafkaProducerPerSmartDriver;
    }

    public void setKafkaProducerPerSmartDriver(boolean kppsd) {
        kafkaProducerPerSmartDriver = kppsd;
    }

    public boolean isRandomizeEachSmartDriverBehaviour() {
        return randomizeEachSmartDriverBehaviour;
    }

    public void setRandomizeEachSmartDriverBehaviour(boolean r) {
        randomizeEachSmartDriverBehaviour = r;
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

    public static synchronized KafkaProducer<String, String> getKafkaMonitoringProducer() {
        return kafkaMonitorigProducer;
    }

    public static synchronized List<LocationLog> getLocationLogList() {
        return locationLogList;
    }

    public static long getStartSimulationTime() {
        return startSimulationTime;
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
