package es.us.lsi.hermes.simulator;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import com.google.maps.model.LatLng;
import es.us.lsi.hermes.analysis.Vehicle;
import es.us.lsi.hermes.csv.CSVLocation;
import es.us.lsi.hermes.csv.CSVSimulatorStatus;
import es.us.lsi.hermes.csv.ICSVBean;
import es.us.lsi.hermes.location.detail.LocationLogDetail;
import es.us.lsi.hermes.google.directions.GeocodedWaypoints;
import es.us.lsi.hermes.google.directions.Location;
import es.us.lsi.hermes.location.LocationLog;
import es.us.lsi.hermes.openStreetMap.PositionSimulatedSpeed;
import es.us.lsi.hermes.person.Person;
import es.us.lsi.hermes.simulator.kafka.Kafka;
import es.us.lsi.hermes.util.*;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Type;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.ResourceBundle;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.mail.MessagingException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.time.DurationFormatUtils;
import org.apache.kafka.clients.producer.KafkaProducer;

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
    static boolean interpolate = true;

    private static ArrayList<LocationLog> locationLogList;

    private static int simulatedSmartDrivers = 1;
    static long startSimulationTime = 0l;
    private static long endSimulationTime = 0l;

    private static int maxSmartDrivers = 20000;

    // Información de monitorización del simulador, para poder generar un CSV y enviarlo por e-mail.
    private static volatile List<ICSVBean> csvStatusList;

    public enum State {
        CONFIG_CHANGED, READY_TO_SIMULATE, SCHEDULED_SIMULATION, SIMULATING, ENDED, INTERRUPTED
    }
    private static State currentState = State.READY_TO_SIMULATE;

    private static volatile SurroundingVehiclesConsumer surroundingVehiclesConsumer;
    private static ConcurrentHashMap<String, SimulatedSmartDriver> simulatedSmartDriverHashMap = new ConcurrentHashMap<>();
    // JYFR: PRUEBA
//    private static ScheduledFuture emergencyScheduler;
    private static ScheduledFuture simulationScheduler;
    private static ScheduledFuture statusMonitorScheduler;

    // Maximum delay among all SmartDrivers.
    private static AtomicLong maxSmartDriversDelayMs;
    // Current mean delay of all SmartDrivers.
    private static AtomicLong currentMeanSmartDriversDelayMs;

    // Directorio temporal para almacenar los archivos generados.
    private static Path tempFolder;

    private final ResourceBundle bundle;

    private static enum Stream_Server {
        KAFKA, ZTREAMY, FIRST_KAFKA_THEN_ZTREAMY, FIRST_ZTREAMY_THEN_KAFKA
    };
    private static Stream_Server streamServer = Stream_Server.KAFKA;

    private static Date scheduledDate;

    // Mecanismos de inicio de los conductores.
    public static enum SmartDrivers_Starting_Mode {
        ALEATORY, LINEAL, SAME_TIME
    };

    // Por defecto, establecemos que el inicio de los usuarios siga una progresión lineal.
    private static SmartDrivers_Starting_Mode startingMode = SmartDrivers_Starting_Mode.LINEAL;

    private static ScheduledThreadPoolExecutor threadPool;

    private static boolean randomizeEachSmartDriverBehaviour = true;

    private static int retries = 5;
    private static boolean offline = false;

    // JYFR: PRUEBA
//    private static boolean infiniteSimulation = false;
    private static boolean infiniteSimulation = true;
    static boolean kafkaProducerPerSmartDriver = true;

    // Kafka
    private static AtomicLong kafkaRecordId;
    private static volatile KafkaProducer<Long, String> kafkaProducer;
    private static volatile KafkaProducer<Long, String> kafkaMonitorigProducer;
    private static Properties kafkaProducerProperties;
    private static Properties kafkaMonitoringProducerProperties;

    public SimulatorController() {
        LOG.log(Level.INFO, "init() - Inicialización del controlador del simulador");

        // Cargamos los recursos de internacionalización.
        this.bundle = ResourceBundle.getBundle("Bundle");

        initAttributes();

        // Iniciamos el 'pool' de hilos de ejecución para los SmartDrivers.
        initThreadPool();

        maxSmartDriversDelayMs = new AtomicLong(0);
        currentMeanSmartDriversDelayMs = new AtomicLong(0);

        // Comprobamos si existe una configuración asignada en el archivo de propiedades y generamos la simulación.
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
        csvStatusList = new ArrayList<>();
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
        relatePathsAndSmartDrivers(PresetSimulation.getPathsAmount());
        setPathsGenerationMethod(PresetSimulation.getPathsGenerationMethod());
        
        // Use pre-calculated routes or get new ones if requested
        if (PresetSimulation.isUseRoutesFromHdd()) {
            CSVUtils.extractSimulatedPaths(); // TODO convert imported to generated
        } else {
            generateSimulatedPaths();
        }

        if (scheduledDate != null) {
            scheduledSimulation();
        }
    }

    public void generateSimulatedPaths() {
        locationLogList = new ArrayList<>();

        // Lista con las tareas de petición de rutas.
        List<Callable<String>> pathRequestTaskList = new ArrayList<>();

        // Crearemos tantas tareas como trayectos se quieran generar.
        for (int i = 0; i < PresetSimulation.getPathsAmount(); i++) {
            final Location destination = PathUtils.getRandomLocation(Constants.SEVILLE.getLat(), Constants.SEVILLE.getLng(), PresetSimulation.getDistanceFromCenter());
            final Location origin = PathUtils.getRandomLocation(destination.getLat(), destination.getLng(), PresetSimulation.getMaxPathDistance());

            // Tarea para la petición de un trayecto.
            Callable callable = () -> {
                String jsonPath = null;
                Location o = origin;
                Location d = destination;
                while (jsonPath == null) {
                    try {
                        if (pathsGenerationMethod.equals(Constants.Paths_Generation_Method.GOOGLE)) {
                            /////////////////
                            // GOOGLE MAPS //
                            /////////////////

                            jsonPath = IOUtils.toString(new URL("https://maps.googleapis.com/maps/api/directions/json?origin=" + o.getLat() + "," + o.getLng() + "&destination=" + d.getLat() + "," + d.getLng()), "UTF-8");
                        } else if (pathsGenerationMethod.equals(Constants.Paths_Generation_Method.OPENSTREETMAP)) {
                            ///////////////////
                            // OPENSTREETMAP //
                            ///////////////////

                            jsonPath = IOUtils.toString(new URL("http://cronos.lbd.org.es/hermes/api/smartdriver/network/simulate?fromLat=" + o.getLat() + "&fromLng=" + o.getLng() + "&toLat=" + d.getLat() + "&toLng=" + d.getLng() + "&speedFactor=1.0"), "UTF-8");
                        }
                    } catch (IOException ex) {
                        LOG.log(Level.SEVERE, "generateSimulatedPaths() - " + pathsGenerationMethod.name() + " - Error I/O: {0}", ex.getMessage());
                        // Generamos nuevos puntos aleatorios hasta que sean aceptados.
                        o = PathUtils.getRandomLocation(Constants.SEVILLE.getLat(), Constants.SEVILLE.getLng(), PresetSimulation.getDistanceFromCenter());
                        d = PathUtils.getRandomLocation(origin.getLat(), origin.getLng(), PresetSimulation.getMaxPathDistance());
                    }
                }

                return jsonPath;
            };

            // Añadimos la tarea al listado de peticiones.            
            pathRequestTaskList.add(callable);
        }

        // Tomamos la marca de tiempo actual. Nos servirá para espaciar las peticiones de trayectos a Google, ya que no se pueden hacer más de 10 peticiones por segundo con la cuenta gratuita.
        // Aplicamos el mismo criterio para OpenStreetMap, aunque no sea necesario en principio.
        long timeMark = System.currentTimeMillis();
        // Ejecutamos el listado de tareas, que se dividirá en los hilos y con las condiciones que haya configurados en 'PathRequestWebService'.
        for (int i = 0; i <= pathRequestTaskList.size(); i += Constants.REQUEST_PACK_SIZE) {
            if (i > 0) {
                long elapsedTime = System.currentTimeMillis() - timeMark;
                if (elapsedTime < 1500) {
                    try {
                        // Antes de hacer la siguiente petición, esperamos 1,5 segundos, para cumplir las restricciones de Google.
                        Thread.sleep(1500 - elapsedTime);
                    } catch (InterruptedException ex) {
                    } finally {
                        timeMark = System.currentTimeMillis();
                    }
                }
                requestPaths(pathRequestTaskList.subList(i - Constants.REQUEST_PACK_SIZE, i));
            }
        }
        int remaining = pathRequestTaskList.size() % Constants.REQUEST_PACK_SIZE;
        if (remaining != 0) {
            try {
                // Antes de hacer la siguiente petición, esperamos 1 segundo, para cumplir las restricciones de Google.
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
            }
            requestPaths(pathRequestTaskList.subList(pathRequestTaskList.size() - remaining, pathRequestTaskList.size()));
        }

        // Stop the listener.
        PathRequestWebService.shutdown();
        currentState = State.READY_TO_SIMULATE;
        if (locationLogList.size() < PresetSimulation.getPathsAmount()) {
            if (PresetSimulation.getPathsAmount() > 0) {
                // Log the generated paths, although the user requested a higher amount.
                LOG.log(Level.SEVERE, "generateSimulatedPaths() - Only {0} paths could be generated", locationLogList.size());
                maxSmartDrivers = Constants.MAX_THREADS / locationLogList.size();
                if (simulatedSmartDrivers > maxSmartDrivers) {
                    simulatedSmartDrivers = maxSmartDrivers;
                }
            } else {
                LOG.log(Level.INFO, bundle.getString("UnableToGetPathsFromService"));
            }
        }
    }

    private void requestPaths(List<Callable<String>> pathRequestTaskSublist) {
        try {
            List<Future<String>> futureTaskList = PathRequestWebService.submitAllTask(pathRequestTaskSublist);
            for (int i = 0; i < futureTaskList.size(); i++) {
                // Creamos un objeto de localizaciones de 'SmartDriver'.
                LocationLog ll = new LocationLog();

                // Procesamos el JSON de respuesta, en función de la plataforma a la que le hayamos hecho la petición.
                try {
                    String json = futureTaskList.get(i).get();

                    if (pathsGenerationMethod.equals(Constants.Paths_Generation_Method.GOOGLE)) {
                        /////////////////
                        // GOOGLE MAPS //
                        /////////////////

                        // Procesamos el JSON obtenido de Google Maps para crear una trayectoria de SmartDriver.
                        Gson gson = new GsonBuilder()
                                .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
                                .create();
                        GeocodedWaypoints gcwp = gson.fromJson(json, GeocodedWaypoints.class);
                        PathUtils.createPathGoogleMaps(gcwp, ll);

                    } else {
                        ///////////////////
                        // OPENSTREETMAP //
                        ///////////////////

                        // Procesamos el JSON obtenido de OpenStreetMap con las localizaciones y las velocidades de SmartDriver.
                        Type listType = new TypeToken<ArrayList<PositionSimulatedSpeed>>() {
                        }.getType();
                        List<PositionSimulatedSpeed> pssList = new Gson().fromJson(json, listType);
                        PathUtils.createPathOpenStreetMaps(pssList, ll);
                    }
                } catch (InterruptedException | ExecutionException | JsonSyntaxException ex) {
                    LOG.log(Level.SEVERE, "Error al decodificar el JSON de la ruta", ex);
                }

                // Si no fuera un trayecto válido, lo ignoramos y pasamos al siguiente
                if (ll.getLocationLogDetailList() == null || ll.getLocationLogDetailList().isEmpty()) {
                    continue;
                }

                // Vemos si se quiere interpolación, para asegurar que haya una localización al menos cada 2.77m, que sería el caso de que un conductor
                // fuera a la velocidad mínima asignada en las simulaciones (10Km/h)
                if (SimulatorController.interpolate) {
                    // Haremos una interpolación lineal para que haya un punto cada 2.77m.
                    ll.setLocationLogDetailList(interpolateLocationLogDetailList(ll.getLocationLogDetailList()));
                }

                // Creamos un usuario simulado, al que le asignaremos el trayecto.
                Person person = createSimPerson();
                ll.setPerson(person);
                ll.setFilename(person.getFullName());

                locationLogList.add(ll);

                // RDL: Once a full route is created, store it on routes folder
                List<ICSVBean> locationList = new ArrayList<>();
                for (LocationLogDetail lld : ll.getLocationLogDetailList()) {
                    locationList.add(new CSVLocation(lld.getLatitude(), lld.getLongitude()));
                }

                CSVUtils.createRouteDataFile(String.valueOf(i + 1), locationList);
            }
        } catch (InterruptedException ex) {
            LOG.log(Level.SEVERE, "Error al obtener el JSON de la ruta", ex);
        }
    }

    private List<LocationLogDetail> interpolateLocationLogDetailList(List<LocationLogDetail> lldList) {
        List<LocationLogDetail> interpolatedLocationLogDetailList = new ArrayList<>();

        for (int i = 0; i < lldList.size() - 1; i++) {
            interpolatedLocationLogDetailList.addAll(interpolateBetween(lldList.get(i), lldList.get(i + 1)));
        }

        return interpolatedLocationLogDetailList;
    }

    public void configChanged() {
        currentState = State.CONFIG_CHANGED;
    }

    private List<LocationLogDetail> interpolateBetween(LocationLogDetail lld1, LocationLogDetail lld2) {
        List<LocationLogDetail> lldListBetween = new ArrayList<>();

        double pointsDistance = Util.distanceHaversine(lld1.getLatitude(), lld1.getLongitude(), lld2.getLatitude(), lld2.getLongitude());

        // Dividimos entre 2.5 para tener incluso más precisión.
        int numberOfInnerLocations = (int) Math.ceil(pointsDistance / 2.5);

        double latitudeFragment = (lld2.getLatitude() - lld1.getLatitude()) / numberOfInnerLocations;
        double longitudeFragment = (lld2.getLongitude() - lld1.getLongitude()) / numberOfInnerLocations;
        double heartRateFragment = (lld2.getHeartRate() - lld1.getHeartRate()) / numberOfInnerLocations;
        double rrFragment = (lld2.getRrTime() - lld1.getRrTime()) / numberOfInnerLocations;
        double speedFragment = (lld2.getSpeed() - lld1.getSpeed()) / numberOfInnerLocations;
        double secondsToBeHereFragment = (lld2.getSecondsToBeHere() - lld1.getSecondsToBeHere()) / numberOfInnerLocations;

        for (int i = 0; i < numberOfInnerLocations; i++) {
            LocationLogDetail lld = new LocationLogDetail();

            lld.setLatitude(i * latitudeFragment + lld1.getLatitude());
            lld.setLongitude(i * longitudeFragment + lld1.getLongitude());
            lld.setSpeed(i * speedFragment + lld1.getSpeed());
            lld.setHeartRate((int) (i * heartRateFragment + lld1.getHeartRate()));
            lld.setRrTime((int) (i * rrFragment + lld1.getRrTime()));
            lld.setSecondsToBeHere((int) (i * secondsToBeHereFragment + lld1.getSecondsToBeHere()));

            lldListBetween.add(lld);
        }

        lldListBetween.add(lld2);

        return lldListBetween;
    }

    private Person createSimPerson() {
        Person person = new Person();
        String name = "Sim_" + System.currentTimeMillis();
        person.setFullName(name);
        person.setEmail(name + "@sim.com");

        return person;
    }

    private void relatePathsAndSmartDrivers(int pathAmount) {
        maxSmartDrivers = Constants.MAX_THREADS / pathAmount;
        if (simulatedSmartDrivers > maxSmartDrivers) {
            simulatedSmartDrivers = maxSmartDrivers;
        }
    }

    public int getMaxSmartDrivers() {
        return maxSmartDrivers;
    }

    public void setPathsGenerationMethod(int value) {
        try {
            pathsGenerationMethod = Constants.Paths_Generation_Method.values()[value];
        } catch (Exception ex) {
            // Si no fuera un valor válido, establecemos un valor por defecto.
            pathsGenerationMethod = Constants.Paths_Generation_Method.GOOGLE;
        }
    }

    public int getSimulatedSmartDrivers() {
        return simulatedSmartDrivers;
    }

    public void setSimulatedSmartDrivers(int ssd) {
        simulatedSmartDrivers = ssd;
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
        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        statusMonitorScheduler = scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                csvStatusList.add(new CSVSimulatorStatus(System.currentTimeMillis(), GENERATED.intValue(), SENT.intValue(), OK.intValue(), NOT_OK.intValue(), ERRORS.intValue(), RECOVERED.intValue(), FINALLY_PENDING.intValue(), threadPool.getQueue().size(), maxSmartDriversDelayMs.get(), currentMeanSmartDriversDelayMs.get()));

                // Evaluate the mean delay.
                long totalDelaysMs = 0l;
                for (SimulatedSmartDriver ssd : simulatedSmartDriverHashMap.values()) {
                    totalDelaysMs += ssd.getCurrentDelayMs();
                }

                currentMeanSmartDriversDelayMs.set(totalDelaysMs / simulatedSmartDriverHashMap.size());
                LOG.log(Level.FINE, "statusMonitorTimer() - SmartDrivers communication with streaming server mean delay in milliseconds: {0}", currentMeanSmartDriversDelayMs.get());
            }
        }, 0, Constants.STATUS_SAMPLING_INTERVAL_S, TimeUnit.SECONDS
        );
    }

    public void simulate() {
        // JYFR: PRUEBA
//            String pattern = bundle.getString("LimitedSimulationTime");
//            JsfUtil.addInfoMessage(MessageFormat.format(pattern, DurationFormatUtils.formatDuration(MAX_SIMULATION_TIME, "HH:mm:ss", true)));
        if (scheduledDate != null) {
            if (simulationScheduler != null) {
                // Había programada una simulación y ha sido cancelada.
                simulationScheduler.cancel(true);
                simulationScheduler = null;
                currentState = State.READY_TO_SIMULATE;
            } else {
                scheduledSimulation();
            }
        } else {
            // Es una simulación sin programar.
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

    private void executeSimulation() {
        currentState = State.SIMULATING;

        kafkaProducer = new KafkaProducer<>(kafkaProducerProperties);
        kafkaMonitorigProducer= new KafkaProducer<>(kafkaMonitoringProducerProperties);
        
        tempFolder = Util.createTempFolder();
        startSimulationTime = System.currentTimeMillis();
        LOG.log(Level.INFO, "executeSimulation() - Comienzo de la simulación: {0}", Constants.dfISO8601.format(startSimulationTime));
        LOG.log(Level.INFO, "executeSimulation() - Envío de tramas a: {0}", Stream_Server.values()[streamServer.ordinal() % 2].name());
        LOG.log(Level.INFO, "executeSimulation() - Condiciones:\n-> Velocidad de simulación: {0}. Ejecución en tiempo real: {1}\n-> ¿Reenviar tramas fallidas?: {2}\n-> Segundos entre reintentos={3}\n-> Modo de inicio de los SmartDrivers={4}", new Object[]{timeRate.name(), timeRate.equals(Time_Rate.X1), PresetSimulation.isRetryOnFail(), PresetSimulation.getIntervalBetweenRetriesInSeconds(), startingMode.name()});
        LOG.log(Level.INFO, "executeSimulation() - Se inicia el consumidor de análisis de vehículos cercanos");
        surroundingVehiclesConsumer = new SurroundingVehiclesConsumer(this);
        surroundingVehiclesConsumer.start();

        // Simulator status monitor init.
        startStatusMonitorTimer();

        // Creation of simulated Smart Drivers.
        LOG.log(Level.INFO, "executeSimulation() - {0} threads are going to be created", simulatedSmartDrivers * locationLogList.size());
        try {
            long id = 0l;
            for (int i = 0; i < locationLogList.size(); i++) {
                LocationLog ll = locationLogList.get(i);
                LocationLogDetail smartDriverPosition = ll.getLocationLogDetailList().get(0);
                LatLng latLng = new LatLng(smartDriverPosition.getLatitude(), smartDriverPosition.getLongitude());

                // Para el caso del modo de inicio LINEAL, si hay más de 10 SmartDrivers, se toma el 10% para repartir su inicio durante 50 segundos.
                int smartDriversBunch = simulatedSmartDrivers > 10 ? (int) (simulatedSmartDrivers * 0.10) : 1;

                LOG.log(Level.FINE, "executeSimulation() - Cada 10 segundos, se iniciarán {0} SmartDrivers en el trayecto {1}", new Object[]{smartDriversBunch, i});
                initSimulatedSmartDriver(id, ll, latLng, smartDriversBunch);
                id++;

                for (int j = 1; j < simulatedSmartDrivers; j++) {
                    initSimulatedSmartDriver(id, ll, latLng, smartDriversBunch);
                    id++;
                }
            }

            // JYFR: PRUEBA
//            LOG.log(Level.INFO, "executeSimulation() - Se activa el sistema de parada de emergencia, si la duración es mayor a: {0}", DurationFormatUtils.formatDuration(MAX_SIMULATION_TIME, "HH:mm:ss", true));
//            startShutdownTimer();
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, "executeSimulation() - Ha ocurrido un problema al crear los hilos de ejecución. Se cancela la simulación", ex);
            // Cancelamos las simulaciones.
            finishSimulation(true);
        }
    }

    private void initSimulatedSmartDriver(long id, LocationLog ll, LatLng latLng, int smartDriversBunch) throws MalformedURLException, HermesException {

        SimulatedSmartDriver ssd = new SimulatedSmartDriver(id, ll, randomizeEachSmartDriverBehaviour, infiniteSimulation, streamServer.ordinal() % 2, retries);
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
        if (ssd.getMaxDelayMs() > maxSmartDriversDelayMs.get()) {
            maxSmartDriversDelayMs.set(ssd.getMaxDelayMs());
            LOG.log(Level.FINE, "smartDriverHasFinished() - Quedan {0} restantes. Máximo retraso detectado hasta ahora: {0}", new Object[]{threadPool.getQueue().size(), maxSmartDriversDelayMs.get()});
        }
    }

    private synchronized void finishSimulation(boolean interrupted) {
        try {
            // JYFR: PRUEBA
//            stopShutdownTimer();
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
                    simulationSummary = MessageFormat.format("RESULTADO DE LA SIMULACION:\n\n-> Servidor de tramas={0}\n\n-> Tramas generadas={1}\n-> Envíos realizados={2}\n-> Oks={3}\n-> NoOks={4}\n-> Errores={5}\n-> Recuperados={6}\n-> No reenviados finalmente={7}\n-> Hilos restantes={8}\n-> Trayectos={9}\n-> Distancia máxima={10}\n-> Instancias SmartDriver por trayecto={11}\n-> Reintentar fallidos={12}\n-> Segundos entre reintentos={13}\n-> Máximo retraso temporal={14}s\n\n", new Object[]{Stream_Server.values()[streamServer.ordinal() % 2].name(), GENERATED, SENT, OK, NOT_OK, ERRORS, RECOVERED, FINALLY_PENDING, threadPool.getQueue().size(), locationLogList.size(), PresetSimulation.getMaxPathDistance(), simulatedSmartDrivers, PresetSimulation.isRetryOnFail(), PresetSimulation.getIntervalBetweenRetriesInSeconds(), Constants.df2Decimals.format(maxSmartDriversDelayMs.get() / 1000.0d)});
                    LOG.log(Level.SEVERE, "finishSimulation() - {0}", simulationSummary);
                } else {
                    simulationSummary = MessageFormat.format("RESULTADO DE LA SIMULACION:\n\nLos envíos se han realizado correctamente:\n\n-> Servidor de tramas={0}\n\n-> Tramas generadas={1}\n-> Oks={2}\n-> Hilos restantes={3}\n-> Trayectos={4}\n-> Distancia máxima={5}\n-> Instancias SmartDriver por trayecto={6}\n-> Reintentar fallidos={7}\n-> Segundos entre reintentos={8}\n-> Máximo retraso temporal={9}s\n\n", new Object[]{Stream_Server.values()[streamServer.ordinal() % 2].name(), GENERATED, OK, threadPool.getQueue().size(), locationLogList.size(), PresetSimulation.getMaxPathDistance(), simulatedSmartDrivers, PresetSimulation.isRetryOnFail(), PresetSimulation.getIntervalBetweenRetriesInSeconds(), Constants.df2Decimals.format(maxSmartDriversDelayMs.get() / 1000.0d)});
                    LOG.log(Level.INFO, "finishSimulation() - {0}", simulationSummary);
                }

                endSimulationTime = System.currentTimeMillis();
                String timeSummary = MessageFormat.format("Inicio de la simulacion: {0} -> Fin de la simulación: {1} ({2})", new Object[]{Constants.dfISO8601.format(startSimulationTime), Constants.dfISO8601.format(endSimulationTime), DurationFormatUtils.formatDuration(endSimulationTime - startSimulationTime, "HH:mm:ss", true)});
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

    public static void logCurrentStatus() {
        LOG.log(Level.SEVERE, "logCurrentStatus() - ESTADO ACTUAL: Tramas generadas={0}|Envíos realizados={1}|Oks={2}|NoOks={3}|Errores={4}|Recuperados={5}|No reenviados finalmente={6}|Hilos restantes={7}|Máximo retraso temporal total={8}ms|Retraso temporal actual={9}ms", new Object[]{GENERATED.get(), SENT.get(), OK.get(), NOT_OK.get(), ERRORS.get(), RECOVERED.get(), FINALLY_PENDING.get(), threadPool.getQueue().size(), maxSmartDriversDelayMs.get(), currentMeanSmartDriversDelayMs.get()});
    }

    public boolean isInterpolate() {
        return interpolate;
    }

    public void setInterpolate(boolean i) {
        interpolate = i;
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
                infiniteSimulation = false;
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

    private static String getComputerName() {
        Map<String, String> env = System.getenv();
        if (env.containsKey("COMPUTERNAME")) {
            return env.get("COMPUTERNAME");
        } else {
            return env.getOrDefault("HOSTNAME", "Unknown");
        }
    }

    // JYFR: PRUEBA
//    private static void stopShutdownTimer() {
//        if (emergencyScheduler != null) {
//            emergencyScheduler.cancel(true);
//        }
//    }
    // JYFR: PRUEBA
//    private void startShutdownTimer() {
//        if (emergencyScheduler != null) {
//            emergencyScheduler.cancel(true);
//        }
//        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
//        // Por seguridad, se establece un tiempo máximo de simulación (más un minuto extra de margen). Cumplido este tiempo se llamará a la finalización de emergencia.
//        emergencyScheduler = scheduledExecutorService.scheduleAtFixedRate(new EmergencyShutdown(startSimulationTime, MAX_SIMULATION_TIME + 60000), 0, 5, TimeUnit.SECONDS);
//    }
    public boolean isInfiniteSimulation() {
        return infiniteSimulation;
    }

    public void setInfiniteSimulation(boolean is) {
        infiniteSimulation = is;
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

    public static void setCurrentSmartDriversDelay(long c) {
        if (c > currentMeanSmartDriversDelayMs.get()) {
            currentMeanSmartDriversDelayMs.set(c);
        }
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
    
    public static synchronized KafkaProducer<Long, String> getKafkaMonitoringProducer() {
        return kafkaMonitorigProducer;
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
