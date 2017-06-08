package es.us.lsi.hermes.simulator;

import com.google.gson.Gson;
import es.us.lsi.hermes.location.LocationLog;
import es.us.lsi.hermes.location.detail.LocationLogDetail;
import es.us.lsi.hermes.kafka.Kafka;
import es.us.lsi.hermes.smartDriver.DataSection;
import es.us.lsi.hermes.smartDriver.RoadSection;
import es.us.lsi.hermes.smartDriver.VehicleLocation;
import es.us.lsi.hermes.util.Constants;
import es.us.lsi.hermes.util.HermesException;
import es.us.lsi.hermes.util.Util;
import es.us.lsi.hermes.ztreamy.Ztreamy;
import java.io.IOException;
import java.net.HttpURLConnection;
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
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang.time.DurationFormatUtils;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import ztreamy.JSONSerializer;
import ztreamy.PublisherHC;

public final class SimulatedSmartDriver extends MonitorizedDriver implements Runnable, ISimulatorControllerObserver {

    private static final Logger LOG = Logger.getLogger(SimulatedSmartDriver.class.getName());

    // Aunque sólo tengamos 2 tipos de eventos, 'VehicleLocation' y 'DataSection', internamente distinguimos entre cuando es un envío normal o cuando es una repetición de un envío previo.
    public enum Event_Type {
        NORMAL_VEHICLE_LOCATION, RECOVERED_VEHICLE_LOCATION, NORMAL_DATA_SECTION, RECOVERED_DATA_SECTION
    }

    // Ztreamy
    private PublisherHC publisher;

    // Kafka
    private long smartDriverKafkaRecordId;
    private KafkaProducer<Long, String> smartDriverKafkaProducer;

    // Parámetros para la simulación.
    private int stressLoad; // Indicará el nivel de carga de estrés.
    private boolean relaxing; // Indicará si el usuario está relajándose tras una carga de estrés.
    private boolean isReversed = false;

    private boolean locationChanged;

    private double sectionDistance;
    private double cummulativePositiveSpeeds;
    private final List<RoadSection> roadSectionList;
    private int ztreamySecondsCount;
    private int secondsBetweenRetries;
    private final int minRrTime;

    private double speedRandomFactor;
    private double hrRandomFactor;

    // Listas con los 'VehicleLocation' y 'DataSection' que han fallado en su envío correspondiente, para poder reintentar su envío.
    private final List<ExtendedEvent> pendingVehicleLocations;
    private final List<ExtendedEvent> pendingDataSections;

    // Identificador del 'FutureTask' correspondiente al hilo del SmartDriver.
    private final long id;
    // Identificador único del SmartDriver.
    private final String sha;

    private final int pathIndex;
    private final int pathPointsCount;

    private final int streamServer;
    private SurroundingVehiclesConsumer surroundingVehiclesConsumer = null;

    private boolean infiniteSimulation;
    private final int retries;

    private boolean paused;

    public SimulatedSmartDriver() {
        this.id = 0;
        this.locationChanged = false;
        this.sectionDistance = 0.0d;
        this.roadSectionList = new ArrayList<>();
        this.cummulativePositiveSpeeds = 0.0d;
        this.ztreamySecondsCount = 0;
        this.stressLoad = 0;
        this.minRrTime = 0;
        this.sha = "";
        this.infiniteSimulation = false;
        this.pendingVehicleLocations = new ArrayList<>();
        this.pendingDataSections = new ArrayList<>();
        this.speedRandomFactor = 0;
        this.hrRandomFactor = 0;
        this.smartDriverKafkaProducer = null;
        this.publisher = null;
        this.retries = 0;
        this.streamServer = 0;
        this.paused = false;
        this.pathIndex = -1;
        this.pathPointsCount = -1;

        init();
    }

    /**
     * Constructor para cada instancia de 'SmartDriver'.
     *
//     TODO Update
//     * @param ll Contendrá los datos de la ruta que debe seguir.
//     * @param randomBehaviour Indicará si tendrá una componente aleatoria en su
     * comportamiento. no.
     * @param infiniteSimulation Indicará si se debe parar la simulación o
     * volver de vuelta cada SmartDriver, cuando llegue a su destino.
     * @param streamServer Indicará el servidor de tramas que recibirá la
     * información de la simulación.
     * @param retries Indicará el número de reintentos de envío de una trama
     * fallida, antes de descartarla.
     *
//     * @throws MalformedURLException
//     * @throws HermesException
     */
    public SimulatedSmartDriver(long id, int pathIndex, int pathPointsCount, boolean infiniteSimulation, int streamServer, int retries, double speedRandomFactor, double hrRandomFactor) throws MalformedURLException, HermesException {
        final SecureRandom random = new SecureRandom();
        this.id = id;
        this.locationChanged = false;
        this.sectionDistance = 0.0d;
        this.roadSectionList = new ArrayList<>();
        this.cummulativePositiveSpeeds = 0.0d;
        this.ztreamySecondsCount = 0;
        this.secondsBetweenRetries = 0;
        this.stressLoad = 0; // Suponemos que inicialmente no está estresado.
        int age = ThreadLocalRandom.current().nextInt(18, 65 + 1); // Simularemos conductores de distintas edades (entre 18 y 65 años), para establecer el ritmo cardíaco máximo en la simulación.
        this.minRrTime = (int) Math.ceil(60000.0d / (220 - age)); // Mínimo R-R, que establecerá el ritmo cardíaco máximo.
        this.sha = new String(Hex.encodeHex(DigestUtils.sha256(random + "@sim.com")));
        this.infiniteSimulation = infiniteSimulation;
        this.streamServer = streamServer;
        this.retries = retries;
        this.paused = false;
        this.pendingVehicleLocations = new ArrayList<>();
        this.pendingDataSections = new ArrayList<>();
        this.pathIndex = pathIndex;
        this.pathPointsCount = pathPointsCount;

//        // TODO: Probar otros timeouts más altos.
        if (PresetSimulation.isKafkaProducerPerSmartDriver()) {
            this.surroundingVehiclesConsumer = new SurroundingVehiclesConsumer(this);
        }

        if (speedRandomFactor == -1 && hrRandomFactor == -1) {
            this.speedRandomFactor = 0.5d + (random.nextDouble() * 1.0d);
            this.hrRandomFactor = 0.9d + (random.nextDouble() * 0.2d);
        } else {
            this.speedRandomFactor = speedRandomFactor;
            this.hrRandomFactor = hrRandomFactor;
        }

//        //FIXME missing - speed tweak by driver
//        for (int i = 0; i < ll.getLocationLogDetailList().size(); i++) {
//            LocationLogDetail lldOriginal = (LocationLogDetail) ll.getLocationLogDetailList().get(i);
//            LocationLogDetail lld = new LocationLogDetail(lldOriginal.getLatitude(), lldOriginal.getLongitude(), lldOriginal.getSpeed(), lldOriginal.getHeartRate(), lldOriginal.getRrTime(), lldOriginal.getSecondsToBeHere());
//
//            lld.setSpeed(lld.getSpeed() * speedRandomFactor);
//            lld.setHeartRate((int) (lld.getHeartRate() * hrRandomFactor));
//
//            // Make sure the speed is bigger or equal to MIN_SPEED.
//            if (lld.getSpeed() < MIN_SPEED) {
//                lld.setSpeed(MIN_SPEED);
//                lld.setSecondsToBeHere((int) (Math.ceil(lld.getSecondsToBeHere() * (lld.getSpeed() / MIN_SPEED))));
//            } else {
//                lld.setSecondsToBeHere((int) (Math.ceil(lld.getSecondsToBeHere() / speedRandomFactor)));
//            }
//
//            localLocationLogDetailList.add(lld);
//        }

        init();
    }

    private void init() {
        if (surroundingVehiclesConsumer != null) {
            surroundingVehiclesConsumer.start();
        }

        switch (streamServer) {
            case 0:
                if (SimulatorController.isKafkaProducerPerSmartDriver()) {
                    // Inicializamos el 'kafkaProducer' de Kafka.
                    Properties kafkaProperties = Kafka.getKafkaProducerProperties();
                    kafkaProperties.setProperty("client.id", sha);
                    this.smartDriverKafkaProducer = new KafkaProducer<>(kafkaProperties);
                }
                break;
            case 1:
                // Inicializamos el 'publisher' de Ztreamy.
                this.publisher = new PublisherHC(Ztreamy.getServerUrl(), new JSONSerializer());
                break;
            default:
                throw new IllegalArgumentException("Invalid Stream Server option");
        }
    }

    public String getSha() {
        return sha;
    }

    private void decreasePendingVehicleLocationsRetries() {
        decreaseEventList(pendingVehicleLocations, Constants.VEHICLE_LOCATION);
    }

    private void decreasePendingDataSectionsRetries() {
        decreaseEventList(pendingDataSections, Constants.DATA_SECTION);
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
        try {
            synchronized (this) {
                while (paused) {
                    wait();
                }
            }

            if (hasFinished()) {
                throw new RuntimeException("Finished SmartDriver");
            }

            if ((PresetSimulation.getMaxSimulationTimeMs() > 0) &&
                    ((System.currentTimeMillis() - SimulatorController.getStartSimulationTime()) >=
                            PresetSimulation.getMaxSimulationTimeMs())) {
                // It has been reached the simulation time.
                finish();
                return;
            }

            // Get the driver's current position on the path
            LocationLogDetail currentLocationLogDetail = getLocationLogDetailByPos(getCurrentPosition());

            relaxing = true;       // Relaxed by default

            LOG.log(Level.FINE, "SimulatedSmartDriver.run() - El usuario de SmartDriver se encuentra en: ({0}, {1})", new Object[]{currentLocationLogDetail.getLatitude(), currentLocationLogDetail.getLongitude()});
            LOG.log(Level.FINE, "SimulatedSmartDriver.run() - Elemento actual: {0} de {1}", new Object[]{getCurrentPosition(), pathPointsCount});

            // Check if it's time to change to the next location
            if (getElapsedSeconds() >= secondsToRemainHere) {
                // If we have arrived to the end of the path, restart or finish the simulation
                if (getCurrentPosition() == (pathPointsCount-1) && direction > 0) {
                    finishOrRepeat();
                }

                // Update the current location
                currentLocationLogDetail = updateCurrentPosition();
            }

            // If it's time to send (every 10 seconds) and the location has changed, do so.
            if (locationChanged && isTimeToSend()) {
                sendEvery10SecondsIfLocationChanged(currentLocationLogDetail);
            // If the simulation allows retries, we have pending data and it's time to send, do so.
            } else if (PresetSimulation.isRetryOnFail() && !pendingVehicleLocations.isEmpty() && isTimeToRetry()) {
                retryPendingVehicleLocations();
            }

            // If the driver has made 500m, send the DataSection
            if (sectionDistance >= Constants.SEND_INTERVAL_METERS) {
                sendDataSection();
            // If the simulation allows retries, we have pending data and it's time to send, do so.
            } else if (PresetSimulation.isRetryOnFail() && !pendingDataSections.isEmpty() && isTimeToRetry()) {
                retryPendingDataSections();
            }

            increaseElapsedSeconds();
            ztreamySecondsCount++;
            if (!pendingVehicleLocations.isEmpty() || !pendingDataSections.isEmpty()) {
                secondsBetweenRetries++;
            }
            LOG.log(Level.FINE, "SimulatedSmartDriver.run() - Elapsed simulation time: {0}", DurationFormatUtils.formatDuration(getElapsedSeconds(), "HH:mm:ss", true));
        } catch (InterruptedException ex) {
            LOG.log(Level.INFO, "SimulatedSmartDriver.run() - Interrupted!");
        }
    }

    private void retryPendingDataSections() {
        /////////////////////////////////////////////////
        // REINTENTO DE ENVÍO DE DATA SECTION FALLIDOS //
        /////////////////////////////////////////////////

        // Aprovechamos que no toca envío de 'DataSection' para probar a enviar los que hubieran fallado.
        increaseSent();
        ExtendedEvent[] events = new ExtendedEvent[pendingDataSections.size()];

        switch (streamServer) {
            case 0:
                // Kafka
                try {
                    String json = new Gson().toJson(events);
                    if (SimulatorController.isKafkaProducerPerSmartDriver()) {
                        smartDriverKafkaProducer.send(new ProducerRecord<>(Kafka.TOPIC_DATA_SECTION,
                                smartDriverKafkaRecordId,
                                json
                        ), new KafkaCallBack(System.currentTimeMillis(), smartDriverKafkaRecordId, events, Event_Type.RECOVERED_DATA_SECTION));
                        smartDriverKafkaRecordId++;
                    } else {
                        long id = SimulatorController.getNextKafkaRecordId();
                        SimulatorController.getKafkaProducer().send(new ProducerRecord<>(Kafka.TOPIC_DATA_SECTION,
                                id,
                                json
                        ), new KafkaCallBack(System.currentTimeMillis(), id, events, Event_Type.RECOVERED_DATA_SECTION));
                    }
                } catch (Exception ex) {
                    LOG.log(Level.SEVERE, "*Reintento* - Error: {0} - No se han podido reenviar los {1} 'DataSection' pendientes", new Object[]{ex.getMessage(), pendingDataSections.size()});
                } finally {
                    secondsBetweenRetries = 0;
                }
                break;
            case 1:
                // ZTreamy
                try {
                    int result = publisher.publish(pendingDataSections.toArray(events), true);
                    if (result == HttpURLConnection.HTTP_OK) {
                        addRecovered(events.length);
                        LOG.log(Level.INFO, "*Reintento* - {0} 'DataSection' pendientes enviados correctamante. SmartDriver: {1}", new Object[]{events.length, sha});
                        pendingDataSections.clear();
                    } else {
                        LOG.log(Level.SEVERE, "*Reintento* - Error SEND (Not OK): No se han podido reenviar los {0} 'DataSection' pendientes", events.length);
                        if (retries != -1) {
                            decreasePendingDataSectionsRetries();
                        }
                        reconnectPublisher();
                    }
                } catch (IOException ex) {
                    LOG.log(Level.SEVERE, "*Reintento* - Error: {0} - No se han podido reenviar los {1} 'DataSection' pendientes", new Object[]{ex.getMessage(), pendingDataSections.size()});
                    reconnectPublisher();
                } finally {
                    secondsBetweenRetries = 0;
                }
                break;
            default:
                throw new IllegalArgumentException("Invalid Stream Server option");
        }
    }

    private void retryPendingVehicleLocations() {
        /////////////////////////////////////////////////////
        // REINTENTO DE ENVÍO DE VEHICLE LOCATION FALLIDOS //
        /////////////////////////////////////////////////////

        // Aprovechamos que no toca envío de 'VehicleLocation' para probar a enviar los que hubieran fallado.
        increaseSent();
        ExtendedEvent[] events = new ExtendedEvent[pendingVehicleLocations.size()];

        switch (streamServer) {
            case 0:
                // Kafka
                try {
                    String json = new Gson().toJson(events);
                    if (SimulatorController.isKafkaProducerPerSmartDriver()) {
                        smartDriverKafkaProducer.send(new ProducerRecord<>(Kafka.TOPIC_VEHICLE_LOCATION,
                                smartDriverKafkaRecordId,
                                json
                        ), new KafkaCallBack(System.currentTimeMillis(), smartDriverKafkaRecordId, events, Event_Type.RECOVERED_VEHICLE_LOCATION));
                        smartDriverKafkaRecordId++;
                    } else {
                        long id = SimulatorController.getNextKafkaRecordId();
                        SimulatorController.getKafkaProducer().send(new ProducerRecord<>(Kafka.TOPIC_VEHICLE_LOCATION,
                                id,
                                json
                        ), new KafkaCallBack(System.currentTimeMillis(), id, events, Event_Type.RECOVERED_VEHICLE_LOCATION));
                    }
                } catch (Exception ex) {
                    LOG.log(Level.SEVERE, "*Reintento* - Error: {0} - No se han podido reenviar los {1} 'VehicleLocation' pendientes", new Object[]{ex.getMessage(), pendingVehicleLocations.size()});
                } finally {
                    secondsBetweenRetries = 0;
                }
                break;
            case 1:
                // Ztreamy
                try {
                    int result = publisher.publish(pendingVehicleLocations.toArray(events), true);
                    if (result == HttpURLConnection.HTTP_OK) {
                        addRecovered(events.length);
                        LOG.log(Level.INFO, "*Reintento* - {0} 'VehicleLocation' pendientes enviadas correctamante. SmartDriver: {1}", new Object[]{events.length, sha});
                        pendingVehicleLocations.clear();
                    } else {
                        LOG.log(Level.SEVERE, "*Reintento* - Error SEND (Not OK): No se han podido reenviar los {0} 'VehicleLocation' pendientes", events.length);
                        if (retries != -1) {
                            decreasePendingVehicleLocationsRetries();
                        }
                        reconnectPublisher();
                    }
                } catch (IOException ex) {
                    LOG.log(Level.SEVERE, "*Reintento* - Error: {0} - No se han podido reenviar los {1} 'VehicleLocation' pendientes", new Object[]{ex.getMessage(), pendingVehicleLocations.size()});
                    reconnectPublisher();
                } finally {
                    secondsBetweenRetries = 0;
                }
                break;
            default:
                throw new IllegalArgumentException("Invalid Stream Server option");
        }
    }

    private LocationLogDetail updateCurrentPosition() {
        int previousPosition = getCurrentPosition();

        // Set the new position (index and direction based)
        setCurrentPosition(previousPosition + direction);

        LOG.log(Level.FINE, "SimulatedSmartDriver.run() - Avanzamos de posición: {0}", getCurrentPosition());
        LocationLogDetail newCurrentPosition = getLocationLogDetailByPos(getCurrentPosition());
        LocationLogDetail previousLocationLogDetail = getLocationLogDetailByPos(previousPosition);
        LOG.log(Level.FINE, "SimulatedSmartDriver.run() - El usuario de SmartDriver se encuentra en: ({0}, {1})",
                new Object[]{newCurrentPosition.getLatitude(), newCurrentPosition.getLongitude()});


        // TODO: Upgrade: ¿Criterios que puedan alterar el estrés?
        // If the current position has a previous one, calculate the difference in bearing and stress the driver.
        if ((previousPosition > 1 && direction > 0) || (previousPosition < (pathPointsCount-1) && direction < 0)) {
            LocationLogDetail antePreviousLocationLogDetail = getLocationLogDetailByPos(previousPosition - direction);
            double currentBearing = Util.bearing(previousLocationLogDetail.getLatitude(),
                    previousLocationLogDetail.getLongitude(), newCurrentPosition.getLatitude(),
                    newCurrentPosition.getLongitude());
            double previousBearing = Util.bearing(antePreviousLocationLogDetail.getLatitude(),
                    antePreviousLocationLogDetail.getLongitude(), previousLocationLogDetail.getLatitude(),
                    previousLocationLogDetail.getLongitude());

            // Si hay una desviación brusca de la trayectoria, suponemos una componente de estrés.
            stressForDeviation(Math.abs(currentBearing - previousBearing));
        }

        // Si hay un salto grande de velocidad, suponemos una componente de estrés.
        stressForSpeed(Math.abs(newCurrentPosition.getSpeed() - previousLocationLogDetail.getSpeed()));

        // Analizamos el ritmo cardíaco,
        // Medimos las unidades de estrés y dibujamos el marker del color correspondiente (verde -> sin estrés, amarillo -> ligeramente estresado, rojo -> estresado)
        if (stressLoad != 0) {
            // Si se está calmando, le subimos el intervalo RR y si se está estresando, le bajamos el intervalo RR.
            if (relaxing) {
                rrTime = rrTime - ((rrTime - Constants.RR_TIME) / stressLoad);
            } else if (stressLoad < 5) {
                rrTime = rrTime - (minRrTime / stressLoad);
            } else {
                // Establecemos un mínimo R-R en función de la edad del conductor.
                rrTime = minRrTime;
            }
        }

        // Calculamos el ritmo cardíaco a partir del intervalo RR.
        heartRate = (int) Math.ceil(60.0d / (rrTime / 1000.0d));

        // Calculate the distance between checkpoints
        double distance = Util.distanceHaversine(previousLocationLogDetail.getLatitude(),
                previousLocationLogDetail.getLongitude(), newCurrentPosition.getLatitude(),
                newCurrentPosition.getLongitude());

        // Acumulamos la distancia recorrida y analizamos el PKE (Positive Kinetic Energy)
        sectionDistance += distance;
        cummulativePositiveSpeeds += analyzePKE(newCurrentPosition, previousLocationLogDetail);

        //FIXME - Review, esto no va debajo de la modificacion de speed y similares??
        // Creamos un elementos de tipo 'RoadSection', para añadirlo al 'DataSection' que se envía a 'Ztreamy' cada 500 metros.
        RoadSection rs = new RoadSection();
        rs.setTime(System.currentTimeMillis());
        rs.setLatitude(newCurrentPosition.getLatitude());
        rs.setLongitude(newCurrentPosition.getLongitude());
        int tDiff = Math.abs(newCurrentPosition.getSecondsToRemainHere() - previousLocationLogDetail.getSecondsToRemainHere());
        rs.setSpeed(tDiff > 0 ? distance * 3.6 / tDiff : previousLocationLogDetail.getSpeed());
        rs.setHeartRate(heartRate);
        rs.setRrTime(rrTime);
        rs.setAccuracy(0);

        roadSectionList.add(rs);

        // Location updated, seconds to remain here updated
        locationChanged = true;
        //FIXME triple check
//        secondsToRemainHere = getLocationLogDetailByPos(getCurrentPosition()).getSecondsToRemainHere() - newCurrentPosition.getSecondsToRemainHere();
        secondsToRemainHere = Math.abs(previousLocationLogDetail.getSecondsToRemainHere() - newCurrentPosition.getSecondsToRemainHere());

        heartRate = (int) (newCurrentPosition.getHeartRate() * hrRandomFactor);
        speed = newCurrentPosition.getSpeed() * speedRandomFactor;

        if (speed < Constants.MIN_SPEED) {
            speed = Constants.MIN_SPEED;
            secondsToRemainHere = (int) (Math.ceil(secondsToRemainHere * (speed / Constants.MIN_SPEED)));
        } else {
            secondsToRemainHere = (int) (Math.ceil(secondsToRemainHere / speedRandomFactor));
        }

        return newCurrentPosition;
    }

    private int rrTime;
    private int heartRate;

    private int direction = 1;  // Controls the direction to iterate over the path.

    private void finishOrRepeat() {
        if (!infiniteSimulation) {
            // Notificamos que ha terminado el SmartDriver actual.
            SimulatorController.smartDriverHasFinished(this.getSha());

            LOG.log(Level.FINE, "SimulatedSmartDriver.run() - El usuario ha llegado a su destino en: {0}", DurationFormatUtils.formatDuration(getElapsedSeconds(), "HH:mm:ss", true));
            finish();

        // When we reach the end in an infinite simulation, turn around and repeat the process
        } else {
            // Calculate the indexes to iterate over the path in one direction or another
            int indexA, indexB;
            if(isReversed) {
                direction = -1;
                indexA = pathPointsCount / 2;
                indexB = 0;
            } else {
                direction = 1;
                indexA = 0;
                indexB = pathPointsCount / 2;
            }
            isReversed = !isReversed;

            resetElapsedSeconds();
        }
    }

    private int secondsToRemainHere = -1;
    private double speed = -1;

    private void stressForDeviation(double bearingDiff) {
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

    private void stressForSpeed(double speedDiff) {
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

    public void stressBySurrounding(int surroundingVehicles) {
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
        return ztreamySecondsCount >= Constants.SEND_INTERVAL_SECONDS;
    }

    private boolean isTimeToRetry() {
        return secondsBetweenRetries >= PresetSimulation.getIntervalBetweenRetriesInSeconds();
    }

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

        //FIXME
        System.out.println("I " + id + " A " + smartDriverLocation + " T " + secondsToRemainHere);

        bodyObject.put("Location", smartDriverLocation);
        increaseGenerated();

        ExtendedEvent event = new ExtendedEvent(sha, "application/json", Constants.SIMULATOR_APPLICATION_ID,
                Constants.VEHICLE_LOCATION, bodyObject, retries);

        increaseSent();
        switch (streamServer) {
            case 0:
                // Kafka
                try {
                    String json = new Gson().toJson(event);
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
                    ztreamySecondsCount = 0;
                }
                break;
            case 1:
                // Ztreamy
                try {
                    int result = publisher.publish(event, true);
                    if (result == HttpURLConnection.HTTP_OK) {
                        increaseOks();
                        LOG.log(Level.FINE, "sendEvery10SecondsIfLocationChanged() - Localización de trayecto simulado enviada correctamante. SmartDriver: {0}", sha);
                        locationChanged = false;
                    } else {
                        increaseNotOks();
                        if (PresetSimulation.isRetryOnFail()) {
                            // Si ha fallado, almacenamos el 'VehicleLocation' que se debería haber enviado y lo intentamos luego.
                            pendingVehicleLocations.add(event);
                        }
                        LOG.log(Level.SEVERE, "sendEvery10SecondsIfLocationChanged() - Error SEND (Not OK)");
                        reconnectPublisher();
                    }
                } catch (MalformedURLException ex) {
                    LOG.log(Level.SEVERE, "sendEvery10SecondsIfLocationChanged() - Error en la URL", ex);
                } catch (Exception ex) {
                    if (!hasFinished()) {
                        increaseErrors();
                        if (PresetSimulation.isRetryOnFail()) {
                            // Si ha fallado, almacenamos el 'VehicleLocation' que se debería haber enviado y lo intentamos luego.
                            pendingVehicleLocations.add(event);
                        }
                        String eType = ex instanceof IOException ? "Error I/O" : "Desconocido";
                        LOG.log(Level.SEVERE, "sendEvery10SecondsIfLocationChanged() - " + eType, ex.getMessage());
                        reconnectPublisher();
                    }
                } finally {
                    // Iniciamos el contador de tiempo para el siguiente envío.
                    ztreamySecondsCount = 0;
                }
                break;
            default:
                throw new IllegalArgumentException("Invalid Stream Server option");
        }
    }

    private void sendDataSection() {
        // Creamos un objeto de tipo 'DataSection' de los que 'SmartDriver' envía al servidor de tramas.
        DataSection dataSection = new DataSection();

        DescriptiveStatistics speedStats = new DescriptiveStatistics();
        DescriptiveStatistics heartRateStats = new DescriptiveStatistics();
        DescriptiveStatistics rrStats = new DescriptiveStatistics();
        DescriptiveStatistics accelerationStats = new DescriptiveStatistics();
        DescriptiveStatistics decelerationStats = new DescriptiveStatistics();
        RoadSection rdPrevious = roadSectionList.get(0);
        speedStats.addValue(rdPrevious.getSpeed());
        rrStats.addValue(rdPrevious.getRrTime());
        int numHighAccelerations = 0;
        int numHighDecelerations = 0;

        for (int i = 1; i < roadSectionList.size(); i++) {
            RoadSection rs = roadSectionList.get(i);
            speedStats.addValue(rs.getSpeed());

            double vDiff = (rs.getSpeed() - rdPrevious.getSpeed()) / 3.6d; // Diferencia de velocidades pasadas a m/s.
            double tDiff = (rs.getTime() - rdPrevious.getTime()) / 1000.0; // Diferencia de tiempos en segundos.
            double acceleration = tDiff > 0.0d ? vDiff / tDiff : 0.0d; // Aceleración o deceleración en m/s2.

            if (acceleration > 0.0d) {
                accelerationStats.addValue(acceleration);
                if (acceleration > Constants.HIGH_ACCELERATION_THRESHOLD) {
                    numHighAccelerations++;
                }
            } else if (acceleration < 0.0d) {
                decelerationStats.addValue(acceleration);
                //TODO Review
                if (numHighDecelerations < Constants.HIGH_DECELERATION_THRESHOLD) {
                    numHighDecelerations++;
                }
            }

            heartRateStats.addValue(rs.getHeartRate());
            rrStats.addValue(rs.getRrTime());

            rdPrevious = rs;
        }
        dataSection.setAverageAcceleration(accelerationStats.getN() > 0 ? (!Double.isNaN(accelerationStats.getMean()) ? accelerationStats.getMean() : 0.0d) : 0.0d);
        dataSection.setAverageDeceleration(decelerationStats.getN() > 0 ? (!Double.isNaN(decelerationStats.getMean()) ? decelerationStats.getMean() : 0.0d) : 0.0d);
        dataSection.setAverageHeartRate(heartRateStats.getN() > 0 ? (!Double.isNaN(heartRateStats.getMean()) ? heartRateStats.getMean() : 0.0d) : 0.0d);
        dataSection.setAverageRR(rrStats.getN() > 0 ? (!Double.isNaN(rrStats.getMean()) ? rrStats.getMean() : 0.0d) : 0.0d);
        dataSection.setAverageSpeed(speedStats.getN() > 0 ? (!Double.isNaN(speedStats.getMean()) ? speedStats.getMean() : 0.0d) : 0.0d);
        dataSection.setNumHighAccelerations(numHighAccelerations);
        dataSection.setNumHighDecelerations(numHighDecelerations);
        dataSection.setMaxSpeed(speedStats.getN() > 0 ? speedStats.getMax() : 0.0d);
        dataSection.setMedianSpeed(speedStats.getN() > 0 ? (!Double.isNaN(speedStats.getPercentile(50)) ? speedStats.getPercentile(50) : 0.0d) : 0.0d);
        dataSection.setMinSpeed(speedStats.getN() > 0 ? speedStats.getMin() : 0.0d);
        dataSection.setPke(sectionDistance > 0.0d ? (cummulativePositiveSpeeds / sectionDistance) : 0.0d);
        List<Integer> rrSectionList = new ArrayList<>();
        for (double rr : rrStats.getValues()) {
            rrSectionList.add((int) rr);
        }
        dataSection.setRrSection(rrSectionList);
        dataSection.setStandardDeviationHeartRate(heartRateStats.getN() > 0 ? (!Double.isNaN(heartRateStats.getStandardDeviation()) ? heartRateStats.getStandardDeviation() : 0.0d) : 0.0d);
        dataSection.setStandardDeviationRR(rrStats.getN() > 0 ? (!Double.isNaN(rrStats.getStandardDeviation()) ? rrStats.getStandardDeviation() : 0.0d) : 0.0d);
        dataSection.setStandardDeviationSpeed(speedStats.getN() > 0 ? (!Double.isNaN(speedStats.getStandardDeviation()) ? speedStats.getStandardDeviation() : 0.0d) : 0.0d);

        // Asignamos la lista de datos del tramo.
        dataSection.setRoadSection(roadSectionList);

        HashMap<String, Object> bodyObject = new HashMap<>();
        bodyObject.put(Constants.DATA_SECTION, dataSection);
        increaseGenerated();

        ExtendedEvent event = new ExtendedEvent(sha, "application/json", Constants.SIMULATOR_APPLICATION_ID, Constants.DATA_SECTION, bodyObject, retries);

        increaseSent();
        switch (streamServer) {
            case 0:
                // Kafka
                try {
                    String json = new Gson().toJson(event);
                    if (SimulatorController.isKafkaProducerPerSmartDriver()) {
                        smartDriverKafkaProducer.send(new ProducerRecord<>(Kafka.TOPIC_DATA_SECTION,
                                smartDriverKafkaRecordId,
                                json
                        ), new KafkaCallBack(System.currentTimeMillis(), smartDriverKafkaRecordId, new ExtendedEvent[]{event}, Event_Type.NORMAL_DATA_SECTION));
                        smartDriverKafkaRecordId++;
                    } else {
                        long id = SimulatorController.getNextKafkaRecordId();
                        SimulatorController.getKafkaProducer().send(new ProducerRecord<>(Kafka.TOPIC_DATA_SECTION,
                                id,
                                json
                        ), new KafkaCallBack(System.currentTimeMillis(), id, new ExtendedEvent[]{event}, Event_Type.NORMAL_DATA_SECTION));
                    }
                } catch (Exception ex) {
                    if (!hasFinished()) {
                        increaseErrors();
                        if (PresetSimulation.isRetryOnFail()) {
                            // Si ha fallado, almacenamos el 'DataSection' que se debería haber enviado y lo intentamos luego.
                            pendingDataSections.add(event);
                        }
                        LOG.log(Level.SEVERE, "sendDataSectionToZtreamy() - Error desconocido: {0} - Primera trama de la sección: {1} - Enviada a las: {2}", new Object[]{ex.getMessage(), dataSection.getRoadSection().get(0).getTimeStamp(), Constants.dfISO8601.format(System.currentTimeMillis())});
                    }
                } finally {
                    // Reiniciamos los acumulados.
                    roadSectionList.clear();
                    cummulativePositiveSpeeds = 0.0d;
                    sectionDistance = 0.0d;
                }
                break;
            case 1:
                // Ztreamy
                try {
                    int result = publisher.publish(event, true);

                    if (result == HttpURLConnection.HTTP_OK) {
                        increaseOks();
                        LOG.log(Level.FINE, "sendDataSectionToZtreamy() - Datos de sección de trayecto simulado enviada correctamante. SmartDriver: {0}", sha);
                    } else {
                        increaseNotOks();
                        if (PresetSimulation.isRetryOnFail()) {
                            // Si ha fallado, almacenamos el 'DataSection' que se debería haber enviado y lo intentamos luego.
                            pendingDataSections.add(event);
                        }
                        LOG.log(Level.SEVERE, "sendDataSectionToZtreamy() - Error SEND (Not OK): Primera trama de la sección: {0} - Enviada a las: {1}", new Object[]{dataSection.getRoadSection().get(0).getTimeStamp(), Constants.dfISO8601.format(System.currentTimeMillis())});
                        reconnectPublisher();
                    }
                } catch (MalformedURLException ex) {
                    LOG.log(Level.SEVERE, "sendDataSectionToZtreamy() - Error en la URL", ex);
                } catch (IOException ex) {
                    if (!hasFinished()) {
                        increaseErrors();
                        if (PresetSimulation.isRetryOnFail()) {
                            // Si ha fallado, almacenamos el 'DataSection' que se debería haber enviado y lo intentamos luego.
                            pendingDataSections.add(event);
                        }
                        LOG.log(Level.SEVERE, "sendDataSectionToZtreamy() - Error I/O: {0} - Primera trama de la sección: {1} - Enviada a las: {2}", new Object[]{ex.getMessage(), dataSection.getRoadSection().get(0).getTimeStamp(), Constants.dfISO8601.format(System.currentTimeMillis())});
                        reconnectPublisher();
                    }
                } catch (Exception ex) {
                    if (!hasFinished()) {
                        increaseErrors();
                        if (PresetSimulation.isRetryOnFail()) {
                            // Si ha fallado, almacenamos el 'DataSection' que se debería haber enviado y lo intentamos luego.
                            pendingDataSections.add(event);
                        }
                        LOG.log(Level.SEVERE, "sendDataSectionToZtreamy() - Error desconocido: {0} - Primera trama de la sección: {1} - Enviada a las: {2}", new Object[]{ex.getMessage(), dataSection.getRoadSection().get(0).getTimeStamp(), Constants.dfISO8601.format(System.currentTimeMillis())});
                        reconnectPublisher();
                    }
                } finally {
                    // Reiniciamos los acumulados.
                    roadSectionList.clear();
                    cummulativePositiveSpeeds = 0.0d;
                    sectionDistance = 0.0d;
                }
                break;
            default:
                throw new IllegalArgumentException("Invalid Stream Server option");
        }
    }

    private double analyzePKE(LocationLogDetail lld, LocationLogDetail lldPrev) {
        // Convertimos los Km/h en m/s.
        double currentSpeedMS = lld.getSpeed() / 3.6d;
        double previousSpeedMS = lldPrev.getSpeed() / 3.6d;

        double speedDifference = currentSpeedMS - previousSpeedMS;
        // Analizamos la diferencia de velocidad.
        if (speedDifference > 0.0d) {
            // Si la diferencia de velocidades es positiva, se tiene en cuenta para el sumatorio.
            return Math.pow(currentSpeedMS, 2) - Math.pow(previousSpeedMS, 2);
        }

        return 0.0d;
    }

    private void reconnectPublisher() {
        publisher.close();
        this.publisher = new PublisherHC(Ztreamy.getServerUrl(), new JSONSerializer());

        LOG.log(Level.FINE, "reconnectPublisher() - Publisher reconnected");
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
            // Si tuviera un 'publisher' de Ztreamy, lo cerramos.
            if (publisher != null) {
                publisher.close();
            }

            surroundingVehiclesConsumer.stopConsumer();
        } catch (Exception ex) {
            // No need to capture
        }
    }

    public double getSpeedRandomFactor() {
        return speedRandomFactor;
    }

    public double getHrRandomFactor() {
        return hrRandomFactor;
    }

    private LocationLog getLocationLog(){
        return SimulatorController.getLocationLogList().get(pathIndex);
    }

    private LocationLogDetail getLocationLogDetailByPos(int pos){
        return getLocationLog().getLocationLogDetailList().get(pos);
    }

    public void setSpeedRandomFactor(double speedRandomFactor) {
        this.speedRandomFactor = speedRandomFactor;
    }

    public void setHrRandomFactor(double hrRandomFactor) {
        this.hrRandomFactor = hrRandomFactor;
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
        return pendingVehicleLocations.size() + pendingDataSections.size();
    }

    public synchronized boolean isPaused() {
        return paused;
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
                    case RECOVERED_DATA_SECTION:
                        addRecovered(events.length);
                        LOG.log(Level.INFO, "*Retry* - {0} Pending 'DataSection' events {1} successfully received. SmartDriver: {2}", new Object[]{events.length, type.name(), sha});
                        pendingDataSections.clear();
                        break;
                    case NORMAL_VEHICLE_LOCATION:
                        increaseOks();
                        LOG.log(Level.FINE, "onCompletion() - 'VehicleLocation' successfully received. SmartDriver: {0}", sha);
                        locationChanged = false;
                        break;
                    case NORMAL_DATA_SECTION:
                        increaseOks();
                        LOG.log(Level.FINE, "onCompletion() - 'DataSection' successfully received. SmartDriver: {0}", sha);
                        break;
                    default:
                        break;
                }
            } else {
                increaseNotOks();
                LOG.log(Level.SEVERE, "onCompletion() - Unable to send message to Kafka", exception);

                switch (type) {
                    case RECOVERED_VEHICLE_LOCATION:
                        if (retries != -1) {
                            // The elements are already in the pending to send list. One retry is subtracted.
                            decreasePendingVehicleLocationsRetries();
                        }
                        break;
                    case RECOVERED_DATA_SECTION:
                        if (retries != -1) {
                            // The elements are already in the pending to send list. One retry is subtracted.
                            decreasePendingDataSectionsRetries();
                        }
                        break;
                    case NORMAL_VEHICLE_LOCATION:
                        increaseErrors();
                        if (PresetSimulation.isRetryOnFail()) {
                            // If fails to send the 'VehicleLocation' stream, it is stored in order to be sent later.
                            pendingVehicleLocations.addAll(Arrays.asList(events));
                        }
                        break;
                    case NORMAL_DATA_SECTION:
                        increaseErrors();
                        if (PresetSimulation.isRetryOnFail()) {
                            // If fails to send the 'DataSection' stream, it is stored in order to be sent later.
                            pendingDataSections.addAll(Arrays.asList(events));
                        }
                        break;
                    default:
                        break;
                }
            }

            // FIXME: Only if necessary. At this moment, it Will be only used the Simulator Status Topic to lighten the simulator.
//            // Finally, it is sent the SmartDriver current status to the streaming server.
//            String json = new Gson().toJson(new SmartDriverStatus(getSha(), System.currentTimeMillis(), currentDelay_ms, metadata != null ? metadata.serializedValueSize() : 0));
//            LOG.log(Level.FINE, "onCompletion() - SmartDriver status JSON: {0}", json);
//            SimulatorController.getKafkaMonitoringProducer().send(new ProducerRecord<>(Kafka.TOPIC_SMARTDRIVER_STATUS, getSha(), json));
        }
    }

    @Override
    public void update(String id, int surroundingSize) {
        if (id.equals(sha)) {
            stressBySurrounding(surroundingSize);
        }
    }
}
