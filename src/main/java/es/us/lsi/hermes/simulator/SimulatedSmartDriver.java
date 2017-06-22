package es.us.lsi.hermes.simulator;

import es.us.lsi.hermes.config.PresetSimulation;
import es.us.lsi.hermes.kafka.ExtendedEvent;
import com.google.gson.Gson;
import es.us.lsi.hermes.location.LocationLogDetail;
import es.us.lsi.hermes.kafka.Kafka;
import es.us.lsi.hermes.topics.VehicleLocation;
import es.us.lsi.hermes.config.Constants;
import es.us.lsi.hermes.util.classes.DriverParameters;
import es.us.lsi.hermes.util.classes.HermesException;
import es.us.lsi.hermes.util.Utils;
import java.net.MalformedURLException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Level;
import java.util.logging.Logger;

import es.us.lsi.hermes.util.classes.ISimulatorControllerObserver;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang.time.DurationFormatUtils;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public final class SimulatedSmartDriver extends MonitorizedDriver implements Runnable, ISimulatorControllerObserver {

    private static final Logger LOG = Logger.getLogger(SimulatedSmartDriver.class.getName());

    // Distinction between regular packaged and retries.
    public enum Event_Type {
        NORMAL_VEHICLE_LOCATION, RECOVERED_VEHICLE_LOCATION
    }

    // Kafka related parameters and constants
    private long smartDriverKafkaRecordId;
    private KafkaProducer<Long, String> smartDriverKafkaProducer;
    private final List<ExtendedEvent> pendingVehicleLocations;     // Failed locations to retry.
    private final long id;      // Identificador del 'FutureTask' correspondiente al hilo del SmartDriver.
    private SurroundingVehiclesConsumer surroundingVehiclesConsumer = null;
    private boolean paused, started;

    // FIXME: Pass to MonitorizedDriver
    private int secondsCount, secondsBetweenRetries;
    private boolean locationChanged;

    // Driver related constants
    private final String sha;   // Identificador único del SmartDriver.
    private final int MAX_RR, MIN_RR;
    private final double speedRandomFactor;
    private final int pathId;
    private final double[] pathPointsSecondsToRemainHere;

    // Driver related params
    private int stressLoad; // Indicará el nivel de carga de estrés.
    private boolean relaxing; // Indicará si el usuario está relajándose tras una carga de estrés.
    private int rrTime;
    private int direction;
    private int speed;

    /**
     * 'SmartDriver' constructor.
     * @param id Unique identifier of the driver.
     * @param pathId Path identifier used by the driver.
     * @param dp Driver parameters that affect driver behaviour.
     * @throws MalformedURLException
     * @throws HermesException 
     */
    public SimulatedSmartDriver(long id, int pathId, DriverParameters dp) throws MalformedURLException, HermesException {
        final SecureRandom random = new SecureRandom();
        this.id = id;
        this.locationChanged = false;
        this.secondsCount = 0;
        this.secondsBetweenRetries = 0;
        this.stressLoad = 0; // Suponemos que inicialmente no está estresado.
        this.sha = new String(Hex.encodeHex(DigestUtils.sha256(random + "@sim.com")));
        this.paused = false;
        this.started = false;
        this.pathId = pathId;
        this.direction = 1;
        this.pendingVehicleLocations = new ArrayList<>();
        this.speedRandomFactor = dp.getSpeedRandomFactor();
        this.speed = 0;

        int age = ThreadLocalRandom.current().nextInt(18, 65 + 1); // Drivers between 18 and 65 years.
        this.rrTime = (int) (Constants.RR_TIME * dp.getHrRandomFactor());
        this.MIN_RR = (int) Math.ceil(60000.0d / (220 - age)); // Min R-R, to establish the max HR.
        this.MAX_RR = (int) Math.ceil(240000.0d / (220 - age)); // Max R-R, to establish the min HR.

        // TODO: Probar otros timeouts más altos.1
        if (PresetSimulation.isKafkaProducerPerSmartDriver()) {
            this.surroundingVehiclesConsumer = new SurroundingVehiclesConsumer(this);
        }
        List<LocationLogDetail> path = SimulatorController.getPath(pathId);
        this.pathPointsSecondsToRemainHere = new double[path.size()];
        for (int position = 0; position < path.size(); position++) {
            pathPointsSecondsToRemainHere[position] = path.get(position).getSecondsToRemainHere() / speedRandomFactor;
        }

        init();
    }

    private void init() {
        if (surroundingVehiclesConsumer != null) {
            surroundingVehiclesConsumer.start();
        }

        if (SimulatorController.isKafkaProducerPerSmartDriver()) {
            // Inicializamos el 'kafkaProducer' de Kafka.
            Properties kafkaProperties = Kafka.getKafkaProducerProperties();
            kafkaProperties.setProperty("client.id", sha);
            this.smartDriverKafkaProducer = new KafkaProducer<>(kafkaProperties);
        }
    }

    public String getSha() {
        return sha;
    }

    private void decreasePendingVehicleLocationsRetries() {
        decreaseEventList(pendingVehicleLocations, Constants.VEHICLE_LOCATION);
    }

    private void decreaseEventList(List<ExtendedEvent> extendedEvents, String eventType) {
        int total = extendedEvents.size();
        for (int i = total - 1; i >= 0; i--) {
            ExtendedEvent ee = extendedEvents.get(i);
            if (ee.getRetries() > 0) {
                ee.decreaseRetries();
            } else {
                extendedEvents.remove(i);
            }
        }
        int discardedEvents = total - extendedEvents.size();
        if (discardedEvents > 0) {
            LOG.log(Level.INFO, "Se han descartado: {0} '" + eventType + "' por alcanzar el máximo número de reintentos de envío", discardedEvents);
        }
    }

    @Override
    public void run() {
        started = true;
        try {
            synchronized (this) {
                while (paused) {
                    wait();
                }
            }

            if (hasFinished()) {
                throw new RuntimeException("Finished SmartDriver");
            }

            // Check if there is a simulation time defined.
            if ((PresetSimulation.getMaxSimulationTimeMs() > 0)
                    && ((System.currentTimeMillis() - SimulatorController.getStartSimulationTime()) >= PresetSimulation.getMaxSimulationTimeMs())) {
                // It has been reached the simulation time.
                finish();
                return;
            }

            // Get the driver's current position on the path
            LocationLogDetail currentLocationLogDetail = SimulatorController.getPath(pathId).get(getCurrentPosition());

            relaxing = true;       // Relaxed by default

            LOG.log(Level.FINE, "SimulatedSmartDriver.run() - El usuario de SmartDriver se encuentra en: ({0}, {1})", new Object[]{currentLocationLogDetail.getLatitude(), currentLocationLogDetail.getLongitude()});

            // Check if we can continue to next location
            if (getPointToPointElapsedSeconds() >= pathPointsSecondsToRemainHere[getCurrentPosition()]) {
                // ¿Have we reached the destination?
                if ((direction > 0 && getCurrentPosition() >= pathPointsSecondsToRemainHere.length - 1)
                        || (direction < 0 && getCurrentPosition() <= 0)) {
                    finishOrRepeat();
                } else {
                    // Update the current location
                    currentLocationLogDetail = updateCurrentPosition(currentLocationLogDetail);
                }
            }

            // If it's time to send (every 10 seconds) and the location has changed, do so.
            if (locationChanged && isTimeToSend()) {
                sendEvery10SecondsIfLocationChanged(currentLocationLogDetail);
                // If the simulation allows retries, we have pending data and it's time to send, do so.
            } else if (PresetSimulation.isRetryOnFail() && !pendingVehicleLocations.isEmpty() && isTimeToRetry()) {
                retryPendingVehicleLocations();
            }

            increaseDriverSimulationTime();
            secondsCount++;
            if (!pendingVehicleLocations.isEmpty()) {
                secondsBetweenRetries++;
            }
            LOG.log(Level.FINE, "SimulatedSmartDriver.run() - Elapsed simulation time: {0}", DurationFormatUtils.formatDuration(getDriverSimulationTimeInSeconds(), "HH:mm:ss", true));

        } catch (InterruptedException ex) {
            LOG.log(Level.INFO, "SimulatedSmartDriver.run() - Interrupted!");
        }
    }

    private void retryPendingVehicleLocations() {
        /////////////////////////////////////////////////////
        // REINTENTO DE ENVÍO DE VEHICLE LOCATION FALLIDOS //
        /////////////////////////////////////////////////////

        // Aprovechamos que no toca envío de 'VehicleLocation' para probar a enviar los que hubieran fallado.
        increaseSent();
        ExtendedEvent[] events = new ExtendedEvent[pendingVehicleLocations.size()];

        // Kafka
        try {
            String json = new Gson().toJson(events);
            if (SimulatorController.isKafkaProducerPerSmartDriver()) {
                smartDriverKafkaProducer.send(
                        new ProducerRecord<>(Kafka.TOPIC_VEHICLE_LOCATION, smartDriverKafkaRecordId, json),
                        new KafkaCallBack(System.currentTimeMillis(), smartDriverKafkaRecordId, events,
                                Event_Type.RECOVERED_VEHICLE_LOCATION));
                smartDriverKafkaRecordId++;
            } else {
                long id = SimulatorController.getNextKafkaRecordId();
                SimulatorController.getKafkaProducer().send(
                        new ProducerRecord<>(Kafka.TOPIC_VEHICLE_LOCATION, id, json),
                        new KafkaCallBack(System.currentTimeMillis(), id, events,
                                Event_Type.RECOVERED_VEHICLE_LOCATION));
            }
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, "*Reintento* - Error: {0} - No se han podido reenviar los {1} 'VehicleLocation' pendientes", new Object[]{ex.getMessage(), pendingVehicleLocations.size()});
        } finally {
            secondsBetweenRetries = 0;
        }
    }

    private void finishOrRepeat() {
        if (!PresetSimulation.isLoopingSimulation()) {
            // Notificamos que ha terminado el SmartDriver actual.
            SimulatorController.smartDriverHasFinished(this.getSha());

            LOG.log(Level.FINE, "SimulatedSmartDriver.run() - El usuario ha llegado a su destino en: {0}", DurationFormatUtils.formatDuration(getPointToPointElapsedSeconds(), "HH:mm:ss", true));
            finish();

        } else {
            // When we reach the end in an infinite simulation, turn around and repeat the process.
            direction *= -1;
            resetPointToPointElapsedSeconds();
        }
    }

    private LocationLogDetail updateCurrentPosition(LocationLogDetail currentLocationLogDetail) {

        int previousPosition = getCurrentPosition();

        double staySeconds = 0.0d;
        boolean nextPositionFound = false;
        // We haven't reached the end, move to the corresponding next location.
        while (!nextPositionFound) {
            setCurrentPosition(getCurrentPosition() + direction);
            staySeconds += pathPointsSecondsToRemainHere[getCurrentPosition()];
            if ((getPointToPointElapsedSeconds() <= staySeconds)
                    || (getCurrentPosition() >= (pathPointsSecondsToRemainHere.length - 1))
                    || (getCurrentPosition() <= 0)) {
                nextPositionFound = true;
            }
        }

        resetPointToPointElapsedSeconds();

        LocationLogDetail previousLocationLogDetail = SimulatorController.getPath(pathId).get(previousPosition);

        // Calculamos la orientación para simular estrés al entrar en una curva.
        double bearing = Utils.bearing(previousLocationLogDetail.getLatitude(), previousLocationLogDetail.getLongitude(), currentLocationLogDetail.getLatitude(), currentLocationLogDetail.getLongitude());

        // FIXME: Review operation.
        if (previousPosition > 1) {
            LocationLogDetail antePreviousLocationLogDetail = SimulatorController.getPath(pathId).get(previousPosition - 1);
            double previousBearing = Utils.bearing(antePreviousLocationLogDetail.getLatitude(), antePreviousLocationLogDetail.getLongitude(), previousLocationLogDetail.getLatitude(), previousLocationLogDetail.getLongitude());
            double bearingDiff = Math.abs(bearing - previousBearing);

            // Si hay una desviación brusca de la trayectoria, suponemos una componente de estrés.
            stressDueToDeviation(bearingDiff);
        }

        double speedDiff = Math.abs(Math.round(currentLocationLogDetail.getSpeed() * speedRandomFactor) - speed);

        // Si hay un salto grande de velocidad, suponemos una componente de estrés.
        stressDueToSpeedChange(speedDiff);

        // If the user is calming down, decrease the HR, else, increase it. (stress based increment and decrement)
        if (relaxing) {
            rrTime -= 20 * (5 - stressLoad);
            if (rrTime < MIN_RR) {
                rrTime = MIN_RR;
            }
        } else {
            rrTime += 20 * stressLoad;
            if (rrTime > MAX_RR) {
                rrTime = MAX_RR;
            }
        }

        // Mark location as changed and update the speed value
        locationChanged = true;
        speed = (int) Math.round(currentLocationLogDetail.getSpeed() * speedRandomFactor);
        if (speed < Constants.MIN_SPEED) {
            speed = Constants.MIN_SPEED;
        }

        return currentLocationLogDetail;
    }

    private void stressDueToDeviation(double bearingDiff) {
        // Graduación del estrés por el cambio de trayectoria
        if (bearingDiff < 25.0d) {
            // Es un tramo 'fácil'.
            if (stressLoad > 0) {
                stressLoad--;
            }
        } else if (bearingDiff < 45.0d) {
            // Es una curva algo cerrada, añadimos un punto de estrés.
            stressLoad++;
            relaxing = false;
        } else if (bearingDiff < 45.0d) {
            // Es una curva cerrada, añadimos 2 punto de estrés.
            stressLoad += 2;
            relaxing = false;
        } else {
            // Es un giro muy cerrado, añadimos 5 punto de estrés.
            stressLoad += 5;
            relaxing = false;
        }
    }

    private void stressDueToSpeedChange(double speedDiff) {
        // Graduación del estrés por cambios de la velocidad
        if (speedDiff < 30.0d) {
            //  Es una variación de velocidad moderada.
            if (stressLoad > 0) {
                stressLoad--;
            }
        } else if (speedDiff < 50.0d) {
            // Es una variación de velocidad alta, añadimos un punto de estrés.
            stressLoad++;
            relaxing = false;
        } else if (speedDiff < 100.0d) {
            // Es una variación de velocidad muy alta, añadimos 2 punto de estrés.
            stressLoad += 2;
            relaxing = false;
        } else {
            // Es una variación de velocidad brusca, añadimos 5 puntos de estrés.
            stressLoad += 5;
            relaxing = false;
        }
    }

    void stressDueToSurrounding(int surroundingVehicles) {
        // Graduación del estrés por cambios de la velocidad
        if (surroundingVehicles < 10) {
            //  Es una variación de velocidad moderada.
            if (stressLoad > 0) {
                stressLoad--;
            }
        } else if (surroundingVehicles < 15) {
            // Es una variación de velocidad alta, añadimos un punto de estrés.
            stressLoad++;
            relaxing = false;
        } else if (surroundingVehicles < 20) {
            // Es una variación de velocidad muy alta, añadimos 2 punto de estrés.
            stressLoad += 2;
            relaxing = false;
        } else {
            // Es una variación de velocidad brusca, añadimos 5 puntos de estrés.
            stressLoad += 5;
            relaxing = false;
        }
    }

    private boolean isTimeToSend() {
        return secondsCount >= Constants.SEND_INTERVAL_SECONDS;
    }

    private boolean isTimeToRetry() {
        return secondsBetweenRetries >= PresetSimulation.getIntervalBetweenRetriesInSeconds();
    }

    private int maxJsonSize = 0;

    private void sendEvery10SecondsIfLocationChanged(LocationLogDetail currentLocationLogDetail) {
        // Creamos un objeto de tipo 'VehicleLocation' de los que 'SmartDriver' envía al servidor de tramas.
        VehicleLocation smartDriverLocation = new VehicleLocation();
        smartDriverLocation.setLatitude(currentLocationLogDetail.getLatitude());
        smartDriverLocation.setLongitude(currentLocationLogDetail.getLongitude());
        smartDriverLocation.setSpeed(speed);
        smartDriverLocation.setAccuracy(0);
        smartDriverLocation.setStress(stressLoad);
        // Asignamos el momento actual del envío de la trama a Ztreamy al LocationLogDetail.
        smartDriverLocation.setTimeStamp(Constants.dfISO8601.format(new Date()));

        HashMap<String, Object> bodyObject = new HashMap<>();

        bodyObject.put("Location", smartDriverLocation);
        increaseGenerated();

        ExtendedEvent event = new ExtendedEvent(sha, "application/json", Constants.SIMULATOR_APPLICATION_ID,
                Constants.VEHICLE_LOCATION, bodyObject, PresetSimulation.getRetries());

        increaseSent();
        try {
            String json = new Gson().toJson(event);

            // Log the Json's biggest size for debugging purposes
            int sizeInBits = json.getBytes("UTF-8").length * 8;
            if (sizeInBits > maxJsonSize) {
                maxJsonSize = sizeInBits;
                LOG.log(Level.FINE, "Maximum Json size till now: {0}", sizeInBits);
            }

            if (SimulatorController.isKafkaProducerPerSmartDriver()) {
                smartDriverKafkaProducer.send(
                        new ProducerRecord<>(Kafka.TOPIC_VEHICLE_LOCATION, smartDriverKafkaRecordId, json),
                        new KafkaCallBack(System.currentTimeMillis(), smartDriverKafkaRecordId,
                                new ExtendedEvent[]{event}, Event_Type.NORMAL_VEHICLE_LOCATION));
                smartDriverKafkaRecordId++;
            } else {
                long id = SimulatorController.getNextKafkaRecordId();
                SimulatorController.getKafkaProducer().send(
                        new ProducerRecord<>(Kafka.TOPIC_VEHICLE_LOCATION, id, json),
                        new KafkaCallBack(System.currentTimeMillis(), id,
                                new ExtendedEvent[]{event}, Event_Type.NORMAL_VEHICLE_LOCATION));
            }
        } catch (Exception ex) {
            if (!hasFinished()) {
                increaseErrors();
                if (PresetSimulation.isRetryOnFail()) {
                    // Si ha fallado, almacenamos el 'VehicleLocation' que se debería haber enviado y lo intentamos luego.
                    pendingVehicleLocations.add(event);
                }
                LOG.log(Level.SEVERE, "sendEvery10SecondsIfLocationChanged() - Error desconocido: {0}", ex);
            }
        } finally {
            // Iniciamos el contador de tiempo para el siguiente envío.
            secondsCount = 0;
        }
    }

    public void finish() {
        setFinished();

        try {
            if (SimulatorController.isKafkaProducerPerSmartDriver()) {
                // Si tuviera un 'producer' de Kafka, lo cerramos.
                if (smartDriverKafkaProducer != null) {
                    smartDriverKafkaProducer.flush();
                    smartDriverKafkaProducer.close();
                    // FIXME: Algunas veces salta una excepción de tipo 'java.lang.InterruptedException'.
                    // Es un 'bug' que aún está en estado aabierto en Kafka.
                    // https://issues.streamsets.com/browse/SDC-4925
                }
            }

            surroundingVehiclesConsumer.stopConsumer();
        } catch (Exception ex) {
            // No need to capture
        }
    }

    public synchronized void pauseSmartDriver() throws InterruptedException {
        paused = true;
    }

    public synchronized void resumeSmartDriver() throws InterruptedException {
        paused = false;
        notify();
    }

    @Override
    public int getPending() {
        return pendingVehicleLocations.size();
    }

    public synchronized boolean isPaused() {
        return paused;
    }

    public boolean isStarted() {
        return started;
    }

    class KafkaCallBack implements Callback {

        private final long startTime;
        private final long key;
        private final ExtendedEvent[] events;
        private final Event_Type type;

        public KafkaCallBack(long startTime, long key, ExtendedEvent[] events, Event_Type type) {
            this.startTime = startTime;
            this.key = key;
            this.events = events;
            this.type = type;
        }

        /**
         * A callback method the user can implement to provide asynchronous
         * handling of request completion. This method will be called when the
         * record sent to the server has been acknowledged. Exactly one of the
         * arguments will be non-null.
         *
         * @param metadata The metadata for the record that was sent (i.e. the
         * partition and offset). Null if an error occurred.
         * @param exception The exception thrown during processing of this
         * record. Null if no error occurred.
         */
        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (metadata != null) {
                // Register the current delay.
                setCurrentDelayMs(System.currentTimeMillis() - startTime);
                LOG.log(Level.FINE, "onCompletion() - Message received in Kafka\n - Key: {0}\n - Events: {1}\n - Partition: {2}\n - Offset: {3}\n - Elapsed time: {4} ms", new Object[]{key, events.length, metadata.partition(), metadata.offset(), getCurrentDelayMs()});

                switch (type) {
                    case RECOVERED_VEHICLE_LOCATION:
                        addRecovered(events.length);
                        LOG.log(Level.INFO, "*Retry* - {0} Pending 'VehicleLocation' events {1} successfully received. SmartDriver: {2}", new Object[]{events.length, type.name(), sha});
                        pendingVehicleLocations.clear();
                        break;
                    case NORMAL_VEHICLE_LOCATION:
                        increaseOks();
                        LOG.log(Level.FINE, "onCompletion() - 'VehicleLocation' successfully received. SmartDriver: {0}", sha);
                        locationChanged = false;
                        break;
                    default:
                        break;
                }
            } else {
                increaseNotOks();
                LOG.log(Level.SEVERE, "onCompletion() - Unable to send " + type.name() + " message to Kafka", exception);

                switch (type) {
                    case RECOVERED_VEHICLE_LOCATION:
                        // The elements are already in the pending to send list. One retry is subtracted.
                        if (PresetSimulation.getRetries() != -1) {
                            decreasePendingVehicleLocationsRetries();
                        }
                        break;
                    case NORMAL_VEHICLE_LOCATION:
                        increaseErrors();
                        if (PresetSimulation.isRetryOnFail()) {
                            // If fails to send the 'VehicleLocation' stream, it is stored in order to be sent later.
                            pendingVehicleLocations.addAll(Arrays.asList(events));
                        }
                        break;
                    default:
                        break;
                }
            }
        }
    }

    @Override
    public void update(String id, int surroundingSize) {
        if (id.equals(sha)) {
            stressDueToSurrounding(surroundingSize);
        }
    }
}
