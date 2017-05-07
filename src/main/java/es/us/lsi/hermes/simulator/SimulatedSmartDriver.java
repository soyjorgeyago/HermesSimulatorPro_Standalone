package es.us.lsi.hermes.simulator;

import com.google.gson.Gson;
import es.us.lsi.hermes.csv.CSVEvent;
import es.us.lsi.hermes.csv.CSVSmartDriverStatus;
import es.us.lsi.hermes.location.LocationLog;
import es.us.lsi.hermes.location.detail.LocationLogDetail;
import es.us.lsi.hermes.simulator.kafka.Kafka;
import es.us.lsi.hermes.smartDriver.DataSection;
import es.us.lsi.hermes.smartDriver.RoadSection;
import es.us.lsi.hermes.util.Constants;
import es.us.lsi.hermes.util.HermesException;
import es.us.lsi.hermes.util.Util;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvBeanWriter;
import org.supercsv.io.ICsvBeanWriter;
import org.supercsv.prefs.CsvPreference;
import ztreamy.JSONSerializer;
import ztreamy.PublisherHC;

public class SimulatedSmartDriver implements Runnable {

    private static final Logger LOG = Logger.getLogger(SimulatedSmartDriver.class.getName());

    // Parámetros recogidos de SmartDriver.
    private static final int ZTREAMY_SEND_INTERVAL_SECONDS = 10;
    private static final int ZTREAMY_SEND_INTERVAL_METERS = 500;
    private static final double HIGH_ACCELERATION_THRESHOLD = 2.5d;
    private static final double HIGH_DECELERATION_THRESHOLD = -3.5d;

    private static final String DATA_SECTION = "Data Section";
    private static final String VEHICLE_LOCATION = "Vehicle Location";

    // Parámetros para la simulación.
    private static final SecureRandom RANDOM = new SecureRandom();
    private int stressLoad; // Indicará el nivel de carga de estrés.
    private boolean relaxing; // Indicará si el usuario está relajándose tras una carga de estrés.
    private static final double MIN_SPEED = 10.0d; // Velocidad mínima de los SmartDrivers.

    // Aunque sólo tengamos 2 tipos de eventos, 'Vehicle Location' y 'Data Section', internamente distinguimos entre cuando es un envío normal o cuando es una repetición de un envío previo.
    public static enum Event_Type {
        NORMAL_VEHICLE_LOCATION, RECOVERED_VEHICLE_LOCATION, NORMAL_DATA_SECTION, RECOVERED_DATA_SECTION
    };

    // Tiempo de simulación transcurrido en segundos del SmartDriver.
    private int elapsedSeconds;

    // Indicará si el vehículo se ha movido.
    private boolean locationChanged;

    // Ztreamy
    private PublisherHC publisher;

    // Kafka
    private long smartDriverKafkaRecordId;
    private KafkaProducer<Long, String> smartDriverKafkaProducer;
    // Lista de hitos del recorrido por las que pasará el SmartDriver.
    private List<LocationLogDetail> localLocationLogDetailList;

    private int currentPosition;
    private boolean finished;
    private final LocationLog ll;
    private double sectionDistance;
    private double cummulativePositiveSpeeds;
    private final List<RoadSection> roadSectionList;
    private int ztreamySecondsCount;
    private int ztreamySecondsBetweenRetries;
    private final int minRrTime;

    // Listas con los 'Vehicle Location' y 'Data Section' que han fallado en su envío correspondiente, para poder reintentar su envío.
    private final List<ExtendedEvent> pendingVehicleLocations;
    private final List<ExtendedEvent> pendingDataSections;

    // Conjunto de eventos enviados con su marca de tiempo del SmartDriver, para el envío del CSV adjunto al terminar la simulación.
    private final List<CSVEvent> csvEventList;

    // Identificador del 'FutureTask' correspondiente al hilo del SmartDriver.
    private final int id;
    // Identificador único del SmartDriver.
    private final String sha;

    private final int streamServer;
//    private final SurroundingVehiclesConsumer surroundingVehiclesConsumer;

    private long maxDelay;
    private long currentDelay;
    private boolean monitorize;
    private boolean infiniteSimulation;
    private final int retries;

    // Información de monitorización del SmartDriver, para poder generar un CSV y enviarlo por e-mail.
    private List<CSVSmartDriverStatus> csvStatusList;

    /**
     * Constructor para cada instancia de 'SmartDriver'.
     *
     * @param ll Contendrá los datos de la ruta que debe seguir.
     * @param pathMarker 'Marker' para mostrar la posición actual en el mapa.
     * @param pathCircle 'Circle' para mostrar la zona de influencia del
     * conductor en el mapa.
     * @param randomBehaviour Indicará si tendrá una componente aleatoria en su
     * comportamiento. no.
     * @param monitorize Indicará si se generará un archivo CSV con la
     * información detallada de cada SmartDriver dureante la simulación.
     * @param infiniteSimulation Indicará si se debe parar la simulación o
     * volver de vuelta cada SmartDriver, cuando llegue a su destino.
     * @param streamServer Indicará el servidor de tramas que recibirá la
     * información de la simulación.
     * @param retries Indicará el número de reintentos de envío de una trama
     * fallida, antes de descartarla.
     *
     * @throws MalformedURLException
     * @throws HermesException
     */
    public SimulatedSmartDriver(int id, LocationLog ll, boolean randomBehaviour, boolean monitorize, boolean infiniteSimulation, int streamServer, int retries) throws MalformedURLException, HermesException {
        this.id = id;
        this.ll = ll;
        this.elapsedSeconds = 0;
        this.locationChanged = false;
        this.currentPosition = 0;
        this.finished = false;
        this.sectionDistance = 0.0d;
        this.roadSectionList = new ArrayList();
        this.cummulativePositiveSpeeds = 0.0d;
        this.ztreamySecondsCount = 0;
        this.ztreamySecondsBetweenRetries = 0;
        this.stressLoad = 0; // Suponemos que inicialmente no está estresado.
        int age = ThreadLocalRandom.current().nextInt(18, 65 + 1); // Simularemos conductores de distintas edades (entre 18 y 65 años), para establecer el ritmo cardíaco máximo en la simulación.
        this.minRrTime = (int) Math.ceil(60000.0d / (220 - age)); // Mínimo R-R, que establecerá el ritmo cardíaco máximo.
        this.sha = new String(Hex.encodeHex(DigestUtils.sha256(System.currentTimeMillis() + ll.getPerson().getEmail())));
        this.maxDelay = 0;
        this.currentDelay = 0;
        this.monitorize = monitorize;
        this.infiniteSimulation = infiniteSimulation;
//        // TODO: Probar otros timeouts más altos.
//        this.surroundingVehiclesConsumer = new SurroundingVehiclesConsumer(Long.parseLong(Kafka.getKafkaProperties().getProperty("consumer.poll.timeout.ms", "1000")), sha, this);
        this.pendingVehicleLocations = new ArrayList<>();
        this.pendingDataSections = new ArrayList<>();

        this.localLocationLogDetailList = new ArrayList<>();

        // Comprobamos si se quiere un comportamiento aleatorio.
        if (randomBehaviour) {
            double speedRandomFactor = 0.5d + (RANDOM.nextDouble() * 1.0d);
            double hrRandomFactor = 0.9d + (RANDOM.nextDouble() * 0.2d);

            for (LocationLogDetail lld : ll.getLocationLogDetailList()) {
                // Aplicamos la variación aleatoria de la velocidad.
                double newSpeed = lld.getSpeed() * speedRandomFactor;
                // Aplicamos la variación aleatoria del ritmo cardíaco.
                int newHr = (int) (lld.getHeartRate() * hrRandomFactor);
                // No habrá ninguna velocidad inferior a la indicada como mínimo.
                if (newSpeed < MIN_SPEED) {
                    localLocationLogDetailList.add(new LocationLogDetail(lld.getLatitude(), lld.getLongitude(), MIN_SPEED, newHr, lld.getRrTime(), (int) (Math.ceil(lld.getSecondsToBeHere() * (lld.getSpeed() / MIN_SPEED)))));
                } else {
                    localLocationLogDetailList.add(new LocationLogDetail(lld.getLatitude(), lld.getLongitude(), newSpeed, newHr, lld.getRrTime(), (int) (Math.ceil(lld.getSecondsToBeHere() / speedRandomFactor))));
                }
            }
        } else {
            // No habrá ninguna velocidad inferior a la indicada como mínimo.
            for (LocationLogDetail lld : ll.getLocationLogDetailList()) {
                if (lld.getSpeed() < MIN_SPEED) {
                    localLocationLogDetailList.add(new LocationLogDetail(lld.getLatitude(), lld.getLongitude(), MIN_SPEED, lld.getHeartRate(), lld.getRrTime(), (int) (Math.ceil(lld.getSecondsToBeHere() * (lld.getSpeed() / MIN_SPEED)))));
                } else {
                    localLocationLogDetailList.add(new LocationLogDetail(lld.getLatitude(), lld.getLongitude(), lld.getSpeed(), lld.getHeartRate(), lld.getRrTime(), lld.getSecondsToBeHere()));
                }
            }
        }
        this.csvEventList = new ArrayList<>();
        this.csvStatusList = new ArrayList<>();
//        this.kafkaRecordId = 0;
        this.streamServer = streamServer;
        switch (streamServer) {
            case 0:
                if (SimulatorController.kafkaProducerPerSmartDriver) {
                    // Inicializamos el 'kafkaProducer' de Kafka.
                    Properties kafkaProperties = Kafka.getKafkaProducerProperties();
                    kafkaProperties.setProperty("client.id", sha);
                    this.smartDriverKafkaProducer = new KafkaProducer<>(kafkaProperties);
                }
                break;
            case 1:
                // Inicializamos el 'publisher' de Ztreamy.
                this.publisher = new PublisherHC(new URL(SimulatorController.ZTREAMY_URL), new JSONSerializer());
                break;
            default:
                throw new IllegalArgumentException("Invalid Stream Server option");
        }
        this.retries = retries;
    }

    public String getSha() {
        return sha;
    }

    private void decreasePendingVehicleLocationsRetries() {
        decreaseEventList(pendingVehicleLocations, "Vehicle Location");
    }

    private void decreasePendingDataSectionsRetries() {
        decreaseEventList(pendingDataSections, "Data Section");
    }

    private void decreaseEventList(List<ExtendedEvent> extendedEvents, String eventType){
        int total = extendedEvents.size();
        for (int i = total - 1; i >= 0; i--) {
            ExtendedEvent ee = extendedEvents.get(i);
            if (ee.getRetries() > 0)
                ee.decreaseRetries();
            else
                extendedEvents.remove(i);
        }
        int discardedEvents = total - extendedEvents.size();
        if (discardedEvents > 0)
            LOG.log(Level.INFO, "Se han descartado: {0} '" + eventType + "' por alcanzar el máximo número de reintentos de envío", discardedEvents);
    }

//    public void startConsumer() {
//        surroundingVehiclesConsumer.start();
//    }
    @Override
    public void run() {
        if (!finished) {
            // Lo primero que comprobamos es si se ha cumplido el tiempo máximo de simulación.
            // Cada hilo comprobará el tiempo que lleva ejecutándose.
            // JYFR: PRUEBA
//            if ((System.currentTimeMillis() - SimulatorController.startSimulationTime) >= SimulatorController.MAX_SIMULATION_TIME) {
//                // Se ha cumplido el tiempo, paramos la ejecución.
//                finish();
//            } else {

                LocationLogDetail currentLocationLogDetail = localLocationLogDetailList.get(currentPosition);

                double distance;
                double bearing;
                // Por defecto, en la simulación se tiende al estado relajado.
                relaxing = true;

                LOG.log(Level.FINE, "SimulatedSmartDriver.run() - El usuario de SmartDriver se encuentra en: ({0}, {1})", new Object[]{currentLocationLogDetail.getLatitude(), currentLocationLogDetail.getLongitude()});
                LOG.log(Level.FINE, "SimulatedSmartDriver.run() - Elemento actual: {0} de {1}", new Object[]{currentPosition, localLocationLogDetailList.size()});

                // Comprobamos si ha pasado suficiente tiempo como para pasar a la siguiente localización.
                if (elapsedSeconds >= currentLocationLogDetail.getSecondsToBeHere()) {
                    // Comprobamos si hemos llegado al destino.
                    if (currentPosition == localLocationLogDetailList.size() - 1) {
                        if (!infiniteSimulation) {
                            // Notificamos que ha terminado el SmartDriver actual.
                            SimulatorController.smartDriverHasFinished(this.getSha());

                            LOG.log(Level.FINE, "SimulatedSmartDriver.run() - El usuario ha llegado a su destino en: {0}", DurationFormatUtils.formatDuration(elapsedSeconds * 1000l, "HH:mm:ss", true));
                            SimulatorController.addFinallyPending(pendingVehicleLocations.size() + pendingDataSections.size());
                            if (monitorize) {
                                SimulatorController.addCSVEvents(csvEventList);
                            }
                            finish();
                        } else {
                            // Hemos llegado al final, pero es una simulación infinita. Le damos la vuelta al recorrido y seguimos.
                            Collections.reverse(localLocationLogDetailList);
                            int size = localLocationLogDetailList.size();
                            for (int i = 0; i < size / 2; i++) {
                                LocationLogDetail lld1 = localLocationLogDetailList.get(i);
                                LocationLogDetail lld2 = localLocationLogDetailList.get(size - 1 - i);
                                int stbh1 = lld1.getSecondsToBeHere();
                                lld1.setSecondsToBeHere(lld2.getSecondsToBeHere());
                                lld2.setSecondsToBeHere(stbh1);
                            }
                            currentPosition = 0;
                            elapsedSeconds = 0;
                        }
                    } else {
                        // No hemos llegado al destino, avanzamos de posición.
                        int previousPosition = currentPosition;
                        for (int i = currentPosition; i < localLocationLogDetailList.size(); i++) {
                            currentPosition = i;
                            if (localLocationLogDetailList.get(i).getSecondsToBeHere() > elapsedSeconds) {
                                break;
                            }
                        }

                        LOG.log(Level.FINE, "SimulatedSmartDriver.run() - Avanzamos de posición: {0}", currentPosition);
                        currentLocationLogDetail = localLocationLogDetailList.get(currentPosition);
                        LOG.log(Level.FINE, "SimulatedSmartDriver.run() - El usuario de SmartDriver se encuentra en: ({0}, {1})", new Object[]{currentLocationLogDetail.getLatitude(), currentLocationLogDetail.getLongitude()});

                        LocationLogDetail previousLocationLogDetail = localLocationLogDetailList.get(previousPosition);

                        // Calculamos la distancia recorrida.
                        distance = Util.distanceHaversine(previousLocationLogDetail.getLatitude(), previousLocationLogDetail.getLongitude(), currentLocationLogDetail.getLatitude(), currentLocationLogDetail.getLongitude());

                        // Calculamos la orientación para simular estrés al entrar en una curva.
                        bearing = Util.bearing(previousLocationLogDetail.getLatitude(), previousLocationLogDetail.getLongitude(), currentLocationLogDetail.getLatitude(), currentLocationLogDetail.getLongitude());

                        // TODO: ¿Criterios que puedan alterar el estrés? 
                        if (previousPosition > 1) {
                            LocationLogDetail antePreviousLocationLogDetail = localLocationLogDetailList.get(previousPosition - 1);
                            double previousBearing = Util.bearing(antePreviousLocationLogDetail.getLatitude(), antePreviousLocationLogDetail.getLongitude(), previousLocationLogDetail.getLatitude(), previousLocationLogDetail.getLongitude());
                            double bearingDiff = Math.abs(bearing - previousBearing);

                            // Si hay una desviación brusca de la trayectoria, suponemos una componente de estrés.
                            stressForDeviation(bearingDiff);

                        }

                        double speedDiff = Math.abs(currentLocationLogDetail.getSpeed() - previousLocationLogDetail.getSpeed());

                        // Si hay un salto grande de velocidad, suponemos una componente de estrés.
                        stressForSpeed(speedDiff);

                        // Analizamos el ritmo cardíaco, 
                        // Medimos las unidades de estrés y dibujamos el marker del color correspondiente (verde -> sin estrés, amarillo -> ligeramente estresado, rojo -> estresado)
                        if (stressLoad == 0) {
                            // No hay estrés.
                        } else {
                            // Si se está calmando, le subimos el intervalo RR y si se está estresando, le bajamos el intervalo RR.
                            if (relaxing) {
                                if (stressLoad > 0) {
                                    currentLocationLogDetail.setRrTime(previousLocationLogDetail.getRrTime() - ((previousLocationLogDetail.getRrTime() - currentLocationLogDetail.getRrTime()) / stressLoad));
                                }
                            } else if (stressLoad < 5) {
                                currentLocationLogDetail.setRrTime(previousLocationLogDetail.getRrTime() - (minRrTime / stressLoad));
                            } else {
                                // Establecemos un mínimo R-R en función de la edad del conductor.
                                currentLocationLogDetail.setRrTime(minRrTime);
                            }

                            if (stressLoad < 5) {
                                // Existe una situación de estrés 'ligero'.
                                // Para que Víctor pueda detectar una situación de estrés, debe haber una diferencia de 50ms en el RR.
                            } else {
                                //  Estrés elevado.
                            }
                        }

                        // Calculamos el ritmo cardíaco a partir del intervalo RR.
                        currentLocationLogDetail.setHeartRate((int) Math.ceil(60.0d / (currentLocationLogDetail.getRrTime() / 1000.0d)));

                        // Acumulamos la distancia recorrida.
                        sectionDistance += distance;

                        // Hacemos el análisis del PKE (Positive Kinetic Energy)
                        cummulativePositiveSpeeds += analyzePKE(currentLocationLogDetail, previousLocationLogDetail);

                        // Creamos un elementos de tipo 'RoadSection', para añadirlo al 'DataSection' que se envía a 'Ztreamy' cada 500 metros.
                        RoadSection rs = new RoadSection();
                        rs.setTime(System.currentTimeMillis());
                        rs.setLatitude(currentLocationLogDetail.getLatitude());
                        rs.setLongitude(currentLocationLogDetail.getLongitude());
                        int tDiff = (currentLocationLogDetail.getSecondsToBeHere() - previousLocationLogDetail.getSecondsToBeHere());
                        if (tDiff > 0) {
                            rs.setSpeed(distance * 3.6 / tDiff);
                        } else {
                            rs.setSpeed(previousLocationLogDetail.getSpeed());
                        }
                        rs.setHeartRate(currentLocationLogDetail.getHeartRate());
                        rs.setRrTime(currentLocationLogDetail.getRrTime());
                        rs.setAccuracy(0);

                        roadSectionList.add(rs);

                        // Hemos cambiado de localización.
                        locationChanged = true;
                    }
                }

                if (locationChanged && isTimeToSend()) {
                    // Sólo si cambiamos de posición y han pasado más de 10 segundos, se envía información a 'Ztreamy'.
                    sendEvery10SecondsIfLocationChanged(currentLocationLogDetail);
                } else if (SimulatorController.retryOnFail && !pendingVehicleLocations.isEmpty()) {

                    // Vemos si ha pasado suficiente tiempo entre reintentos.
                    if (isTimeToRetry()) {
                        /////////////////////////////////////////////////////
                        // REINTENTO DE ENVÍO DE VEHICLE LOCATION FALLIDOS //
                        /////////////////////////////////////////////////////

                        // Aprovechamos que no toca envío de 'Vehicle Location' para probar a enviar los que hubieran fallado.
                        SimulatorController.increaseSends();
                        ExtendedEvent[] events = new ExtendedEvent[pendingVehicleLocations.size()];

                        switch (streamServer) {
                            case 0:
                                // Kafka
                                try {
                                    String json = new Gson().toJson(events);
                                    long id = SimulatorController.getNextKafkaRecordId();
                                    // JYFR: PRUEBA
                                    LOG.log(Level.INFO, "JSON: {0}", json);
                                    if (SimulatorController.kafkaProducerPerSmartDriver) {
                                        smartDriverKafkaProducer.send(new ProducerRecord<>(Kafka.TOPIC_VEHICLE_LOCATION,
                                                smartDriverKafkaRecordId,
                                                json
                                        ), new KafkaCallBack(System.currentTimeMillis(), smartDriverKafkaRecordId, events, Event_Type.RECOVERED_VEHICLE_LOCATION));
                                        smartDriverKafkaRecordId++;
                                    } else {
                                        SimulatorController.getKafkaProducer().send(new ProducerRecord<>(Kafka.TOPIC_VEHICLE_LOCATION,
                                                id,
                                                json
                                        ), new KafkaCallBack(System.currentTimeMillis(), id, events, Event_Type.RECOVERED_VEHICLE_LOCATION));
                                    }
                                } catch (Exception ex) {
                                    LOG.log(Level.SEVERE, "*Reintento* - Error: {0} - No se han podido reenviar los {1} 'Vehicle Location' pendientes", new Object[]{ex.getMessage(), pendingVehicleLocations.size()});
                                    SimulatorController.logCurrentStatus();
                                } finally {
                                    ztreamySecondsBetweenRetries = 0;
                                }
                                break;
                            case 1:
                                // Ztreamy
                                try {
                                    int result = publisher.publish(pendingVehicleLocations.toArray(events), true);
                                    if (result == HttpURLConnection.HTTP_OK) {
                                        SimulatorController.addRecovered(events.length);
                                        LOG.log(Level.INFO, "*Reintento* - {0} 'Vehicle Location' pendientes enviadas correctamante. SmartDriver: {1}", new Object[]{events.length, ll.getPerson().getEmail()});
                                        pendingVehicleLocations.clear();
                                    } else {
                                        LOG.log(Level.SEVERE, "*Reintento* - Error SEND (Not OK): No se han podido reenviar los {0} 'Vehicle Location' pendientes", events.length);
                                        SimulatorController.logCurrentStatus();
                                        if (retries != -1) {
                                            decreasePendingVehicleLocationsRetries();
                                        }
                                        reconnectPublisher();
                                    }
                                } catch (IOException ex) {
                                    LOG.log(Level.SEVERE, "*Reintento* - Error: {0} - No se han podido reenviar los {1} 'Vehicle Location' pendientes", new Object[]{ex.getMessage(), pendingVehicleLocations.size()});
                                    SimulatorController.logCurrentStatus();
                                    reconnectPublisher();
                                } finally {
                                    ztreamySecondsBetweenRetries = 0;
                                }
                                break;
                            default:
                                throw new IllegalArgumentException("Invalid Stream Server option");
                        }
                    }
                }

                // Se enviará un resumen cada 500 metros.
                if (sectionDistance >= ZTREAMY_SEND_INTERVAL_METERS) {
                    sendDataSection();
                } else if (SimulatorController.retryOnFail && !pendingDataSections.isEmpty()) {

                    // Vemos si ha pasado suficiente tiempo entre reintentos.
                    if (isTimeToRetry()) {
                        /////////////////////////////////////////////////
                        // REINTENTO DE ENVÍO DE DATA SECTION FALLIDOS //
                        /////////////////////////////////////////////////

                        // Aprovechamos que no toca envío de 'Data Section' para probar a enviar los que hubieran fallado.
                        SimulatorController.increaseSends();
                        ExtendedEvent[] events = new ExtendedEvent[pendingDataSections.size()];

                        switch (streamServer) {
                            case 0:
                                // Kafka
                                try {
                                    String json = new Gson().toJson(events);
                                    long id = SimulatorController.getNextKafkaRecordId();
                                    // JYFR: PRUEBA
                                    LOG.log(Level.INFO, "JSON: {0}", json);
                                    if (SimulatorController.kafkaProducerPerSmartDriver) {
                                        smartDriverKafkaProducer.send(new ProducerRecord<>(Kafka.TOPIC_DATA_SECTION,
                                                smartDriverKafkaRecordId,
                                                json
                                        ), new KafkaCallBack(System.currentTimeMillis(), smartDriverKafkaRecordId, events, Event_Type.RECOVERED_DATA_SECTION));
                                        smartDriverKafkaRecordId++;
                                    } else {
                                        SimulatorController.getKafkaProducer().send(new ProducerRecord<>(Kafka.TOPIC_DATA_SECTION,
                                                id,
                                                json
                                        ), new KafkaCallBack(System.currentTimeMillis(), id, events, Event_Type.RECOVERED_DATA_SECTION));
                                    }
                                } catch (Exception ex) {
                                    LOG.log(Level.SEVERE, "*Reintento* - Error: {0} - No se han podido reenviar los {1} 'Data Section' pendientes", new Object[]{ex.getMessage(), pendingDataSections.size()});
                                    SimulatorController.logCurrentStatus();
                                } finally {
                                    ztreamySecondsBetweenRetries = 0;
                                }
                                break;
                            case 1:
                                // ZTreamy
                                try {
                                    int result = publisher.publish(pendingDataSections.toArray(events), true);
                                    if (result == HttpURLConnection.HTTP_OK) {
                                        SimulatorController.addRecovered(events.length);
                                        LOG.log(Level.INFO, "*Reintento* - {0} 'Data Section' pendientes enviados correctamante. SmartDriver: {1}", new Object[]{events.length, ll.getPerson().getEmail()});
                                        pendingDataSections.clear();
                                    } else {
                                        LOG.log(Level.SEVERE, "*Reintento* - Error SEND (Not OK): No se han podido reenviar los {0} 'Data Section' pendientes", events.length);
                                        SimulatorController.logCurrentStatus();
                                        if (retries != -1) {
                                            decreasePendingDataSectionsRetries();
                                        }
                                        reconnectPublisher();
                                    }
                                } catch (IOException ex) {
                                    LOG.log(Level.SEVERE, "*Reintento* - Error: {0} - No se han podido reenviar los {1} 'Data Section' pendientes", new Object[]{ex.getMessage(), pendingDataSections.size()});
                                    SimulatorController.logCurrentStatus();
                                    reconnectPublisher();
                                } finally {
                                    ztreamySecondsBetweenRetries = 0;
                                }
                                break;
                            default:
                                throw new IllegalArgumentException("Invalid Stream Server option");
                        }
                    }
                }

                elapsedSeconds++;
                ztreamySecondsCount++;
                if (!pendingVehicleLocations.isEmpty() || !pendingDataSections.isEmpty()) {
                    ztreamySecondsBetweenRetries++;
                }
                LOG.log(Level.FINE, "SimulatedSmartDriver.run() - Tiempo de simulación transcurrido: {0}", DurationFormatUtils.formatDuration(elapsedSeconds * 1000l, "HH:mm:ss", true));
                // JYFR: PRUEBA
//            }
        } else {
            throw new RuntimeException("Finished SmartDriver");
        }
    }

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

    private boolean isTimeToSend() {
        return ztreamySecondsCount >= ZTREAMY_SEND_INTERVAL_SECONDS;
    }

    private boolean isTimeToRetry() {
        return ztreamySecondsBetweenRetries >= SimulatorController.secondsBetweenRetries;
    }

    private void sendEvery10SecondsIfLocationChanged(LocationLogDetail currentLocationLogDetail) {
        // Creamos un objeto de tipo 'Location' de los que 'SmartDriver' envía al servidor de tramas.
        es.us.lsi.hermes.smartDriver.Location smartDriverLocation = new es.us.lsi.hermes.smartDriver.Location();
        smartDriverLocation.setLatitude(currentLocationLogDetail.getLatitude());
        smartDriverLocation.setLongitude(currentLocationLogDetail.getLongitude());
        smartDriverLocation.setSpeed(currentLocationLogDetail.getSpeed());
        smartDriverLocation.setAccuracy(0);
        smartDriverLocation.setScore(0);
        // Asignamos el momento actual del envío de la trama a Ztreamy al LocationLogDetail.
        smartDriverLocation.setTimeStamp(Constants.dfISO8601.format(new Date()));

        HashMap<String, Object> bodyObject = new HashMap<>();
        bodyObject.put("Location", smartDriverLocation);
        SimulatorController.increaseGenerated();

        ExtendedEvent event = new ExtendedEvent(sha, "application/json", Constants.SIMULATOR_APPLICATION_ID, VEHICLE_LOCATION, bodyObject, retries);

        SimulatorController.increaseSends();
        switch (streamServer) {
            case 0:
                // Kafka
                try {
                    String json = new Gson().toJson(event);
                    long id = SimulatorController.getNextKafkaRecordId();
                    // JYFR: PRUEBA
                    LOG.log(Level.INFO, "JSON: {0}", json);
                    if (SimulatorController.kafkaProducerPerSmartDriver) {
                        smartDriverKafkaProducer.send(new ProducerRecord<>(Kafka.TOPIC_VEHICLE_LOCATION,
                                smartDriverKafkaRecordId,
                                json
                        ), new KafkaCallBack(System.currentTimeMillis(), smartDriverKafkaRecordId, new ExtendedEvent[]{event}, Event_Type.NORMAL_VEHICLE_LOCATION));
                        smartDriverKafkaRecordId++;
                    } else {
                        SimulatorController.getKafkaProducer().send(new ProducerRecord<>(Kafka.TOPIC_VEHICLE_LOCATION,
                                id,
                                json
                        ), new KafkaCallBack(System.currentTimeMillis(), id, new ExtendedEvent[]{event}, Event_Type.NORMAL_VEHICLE_LOCATION));
                    }
                } catch (Exception ex) {
                    if (!finished) {
                        SimulatorController.increaseErrors();
                        if (SimulatorController.retryOnFail) {
                            // Si ha fallado, almacenamos el 'Vehicle Location' que se debería haber enviado y lo intentamos luego.
                            pendingVehicleLocations.add(event);
                        }
                        LOG.log(Level.SEVERE, "sendEvery10SecondsIfLocationChanged() - Error desconocido: {0}", ex.getMessage());
                        SimulatorController.logCurrentStatus();
                    }
                } finally {
                    // Iniciamos el contador de tiempo para el siguiente envío.
                    ztreamySecondsCount = 0;
                    if (monitorize) {
                        csvEventList.add(new CSVEvent(event.getEventId(), event.getTimestamp()));
                    }
                }
                break;
            case 1:
                // Ztreamy
                try {
                    int result = publisher.publish(event, true);
                    if (result == HttpURLConnection.HTTP_OK) {
                        SimulatorController.increaseOkSends();
                        LOG.log(Level.FINE, "sendEvery10SecondsIfLocationChanged() - Localización de trayecto simulado enviada correctamante. SmartDriver: {0}", ll.getPerson().getEmail());
                        locationChanged = false;
                    } else {
                        SimulatorController.increaseNoOkSends();
                        if (SimulatorController.retryOnFail) {
                            // Si ha fallado, almacenamos el 'Vehicle Location' que se debería haber enviado y lo intentamos luego.
                            pendingVehicleLocations.add(event);
                        }
                        LOG.log(Level.SEVERE, "sendEvery10SecondsIfLocationChanged() - Error SEND (Not OK)");
                        SimulatorController.logCurrentStatus();
                        reconnectPublisher();
                    }
                } catch (MalformedURLException ex) {
                    LOG.log(Level.SEVERE, "sendEvery10SecondsIfLocationChanged() - Error en la URL", ex);
                } catch (IOException ex) {
                    if (!finished) {
                        SimulatorController.increaseErrors();
                        if (SimulatorController.retryOnFail) {
                            // Si ha fallado, almacenamos el 'Vehicle Location' que se debería haber enviado y lo intentamos luego.
                            pendingVehicleLocations.add(event);
                        }
                        LOG.log(Level.SEVERE, "sendEvery10SecondsIfLocationChanged() - Error I/O: {0}", ex.getMessage());
                        SimulatorController.logCurrentStatus();
                        reconnectPublisher();
                    }
                } catch (Exception ex) {
                    if (!finished) {
                        SimulatorController.increaseErrors();
                        if (SimulatorController.retryOnFail) {
                            // Si ha fallado, almacenamos el 'Vehicle Location' que se debería haber enviado y lo intentamos luego.
                            pendingVehicleLocations.add(event);
                        }
                        LOG.log(Level.SEVERE, "sendEvery10SecondsIfLocationChanged() - Error desconocido: {0}", ex.getMessage());
                        SimulatorController.logCurrentStatus();
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
                if (acceleration > HIGH_ACCELERATION_THRESHOLD) {
                    numHighAccelerations++;
                }
            } else if (acceleration < 0.0d) {
                decelerationStats.addValue(acceleration);
                if (numHighDecelerations < HIGH_DECELERATION_THRESHOLD) {
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
        List<Integer> rrSectionList = new ArrayList();
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
        bodyObject.put(DATA_SECTION, dataSection);
        SimulatorController.increaseGenerated();

        ExtendedEvent event = new ExtendedEvent(sha, "application/json", Constants.SIMULATOR_APPLICATION_ID, DATA_SECTION, bodyObject, retries);
        csvEventList.add(new CSVEvent(event.getEventId(), event.getTimestamp()));

        SimulatorController.increaseSends();
        switch (streamServer) {
            case 0:
                // Kafka
                try {
                    String json = new Gson().toJson(event);
                    long id = SimulatorController.getNextKafkaRecordId();
                    // JYFR: PRUEBA
                    LOG.log(Level.INFO, "JSON: {0}", json);
                    if (SimulatorController.kafkaProducerPerSmartDriver) {
                        smartDriverKafkaProducer.send(new ProducerRecord<>(Kafka.TOPIC_DATA_SECTION,
                                smartDriverKafkaRecordId,
                                json
                        ), new KafkaCallBack(System.currentTimeMillis(), smartDriverKafkaRecordId, new ExtendedEvent[]{event}, Event_Type.NORMAL_DATA_SECTION));
                        smartDriverKafkaRecordId++;
                    } else {
                        SimulatorController.getKafkaProducer().send(new ProducerRecord<>(Kafka.TOPIC_DATA_SECTION,
                                id,
                                json
                        ), new KafkaCallBack(System.currentTimeMillis(), id, new ExtendedEvent[]{event}, Event_Type.NORMAL_DATA_SECTION));
                    }
                } catch (Exception ex) {
                    if (!finished) {
                        SimulatorController.increaseErrors();
                        if (SimulatorController.retryOnFail) {
                            // Si ha fallado, almacenamos el 'Data Section' que se debería haber enviado y lo intentamos luego.
                            pendingDataSections.add(event);
                        }
                        LOG.log(Level.SEVERE, "sendDataSectionToZtreamy() - Error desconocido: {0} - Primera trama de la sección: {1} - Enviada a las: {2}", new Object[]{ex.getMessage(), dataSection.getRoadSection().get(0).getTimeStamp(), Constants.dfISO8601.format(System.currentTimeMillis())});
                        SimulatorController.logCurrentStatus();
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
                        SimulatorController.increaseOkSends();
                        LOG.log(Level.FINE, "sendDataSectionToZtreamy() - Datos de sección de trayecto simulado enviada correctamante. SmartDriver: {0}", ll.getPerson().getEmail());
                    } else {
                        SimulatorController.increaseNoOkSends();
                        if (SimulatorController.retryOnFail) {
                            // Si ha fallado, almacenamos el 'Data Section' que se debería haber enviado y lo intentamos luego.
                            pendingDataSections.add(event);
                        }
                        LOG.log(Level.SEVERE, "sendDataSectionToZtreamy() - Error SEND (Not OK): Primera trama de la sección: {0} - Enviada a las: {1}", new Object[]{dataSection.getRoadSection().get(0).getTimeStamp(), Constants.dfISO8601.format(System.currentTimeMillis())});
                        SimulatorController.logCurrentStatus();
                        reconnectPublisher();
                    }
                } catch (MalformedURLException ex) {
                    LOG.log(Level.SEVERE, "sendDataSectionToZtreamy() - Error en la URL", ex);
                } catch (IOException ex) {
                    if (!finished) {
                        SimulatorController.increaseErrors();
                        if (SimulatorController.retryOnFail) {
                            // Si ha fallado, almacenamos el 'Data Section' que se debería haber enviado y lo intentamos luego.
                            pendingDataSections.add(event);
                        }
                        LOG.log(Level.SEVERE, "sendDataSectionToZtreamy() - Error I/O: {0} - Primera trama de la sección: {1} - Enviada a las: {2}", new Object[]{ex.getMessage(), dataSection.getRoadSection().get(0).getTimeStamp(), Constants.dfISO8601.format(System.currentTimeMillis())});
                        SimulatorController.logCurrentStatus();
                        reconnectPublisher();
                    }
                } catch (Exception ex) {
                    if (!finished) {
                        SimulatorController.increaseErrors();
                        if (SimulatorController.retryOnFail) {
                            // Si ha fallado, almacenamos el 'Data Section' que se debería haber enviado y lo intentamos luego.
                            pendingDataSections.add(event);
                        }
                        LOG.log(Level.SEVERE, "sendDataSectionToZtreamy() - Error desconocido: {0} - Primera trama de la sección: {1} - Enviada a las: {2}", new Object[]{ex.getMessage(), dataSection.getRoadSection().get(0).getTimeStamp(), Constants.dfISO8601.format(System.currentTimeMillis())});
                        SimulatorController.logCurrentStatus();
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
        try {
            this.publisher = new PublisherHC(new URL(SimulatorController.ZTREAMY_URL), new JSONSerializer());
        } catch (MalformedURLException e) {
            // No puede pasar, porque habría pasado también en el constructor
            // y no lo ha hecho.
        }
        LOG.log(Level.FINE, "reconnectPublisher() - Publisher reconnected");
    }

    public void finish() {
        finished = true;
        try {
            if (SimulatorController.kafkaProducerPerSmartDriver) {
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
        } catch (Exception ex) {
        } finally {
            if (monitorize) {
                // Creamos un archivo temporal para el CSV con la información del SmartDriver.
                String statusFileName = Constants.dfFile.format(System.currentTimeMillis());
                String statusFileNameCSV = statusFileName + "_smartDriver_status.csv";
                LOG.log(Level.INFO, "generateZippedCSV() - Generando archivo CSV con la información del SmartDriver: {0}", statusFileNameCSV);
                File statusFile = new File(SimulatorController.getTempFolder().toUri().getPath(), statusFileNameCSV);
                createStatusDataFile(CsvPreference.EXCEL_NORTH_EUROPE_PREFERENCE, false, statusFile);
            }
        }
    }

    public long getMaxDelay() {
        return maxDelay;
    }

    private void createStatusDataFile(CsvPreference csvPreference, boolean ignoreHeaders, File file) {
        ICsvBeanWriter beanWriter = null;

        if (csvStatusList != null && !csvStatusList.isEmpty()) {
            try {

                beanWriter = new CsvBeanWriter(new FileWriter(file), csvPreference);

                CSVSmartDriverStatus bean = csvStatusList.get(0);
                // Seleccionamos los atributos que vamos a exportar.
                final String[] fields = bean.getFields();

                // Aplicamos las características de los campos.
                final CellProcessor[] processors = bean.getProcessors();

                if (!ignoreHeaders) {
                    // Ponemos la cabecera con los nombres de los atributos.
                    if (bean.getHeaders() != null) {
                        beanWriter.writeHeader(bean.getHeaders());
                    } else {
                        beanWriter.writeHeader(fields);
                    }
                }

                // Procesamos los elementos.
                for (final CSVSmartDriverStatus element : csvStatusList) {
                    beanWriter.write(element, fields, processors);
                }
            } catch (IOException ex) {
                LOG.log(Level.SEVERE, "createStatusDataFile() - Error al exportar a CSV la información de los estados del SmartDriver", ex);
            } finally {
                // Cerramos.
                if (beanWriter != null) {
                    try {
                        beanWriter.close();
                    } catch (IOException ex) {
                        LOG.log(Level.SEVERE, "createStatusDataFile() - Error al cerrar el 'writer'", ex);
                    }
                }
            }
        }
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
                currentDelay = System.currentTimeMillis() - startTime;
                SimulatorController.setCurrentSmartDriversDelay(currentDelay);
                // Registramos el retraso máximo.
                if (currentDelay > maxDelay) {
                    maxDelay = currentDelay;
                }

                if (monitorize) {
                    // Registramos el estado del SmartDriver.
                    csvStatusList.add(new CSVSmartDriverStatus(id, System.currentTimeMillis(), currentDelay, metadata.serializedValueSize()));
                }

                LOG.log(Level.FINE, "onCompletion() - Mensaje recibido correctamente en Kafka\n - Key: {0}\n - Número de eventos: {1}\n - Partición: {2}\n - Offset: {3}\n - Tiempo transcurrido: {4} ms", new Object[]{key, events.length, metadata.partition(), metadata.offset(), currentDelay});
                switch (type) {
                    case RECOVERED_VEHICLE_LOCATION:
                    case RECOVERED_DATA_SECTION:
                        SimulatorController.addRecovered(events.length);
                        LOG.log(Level.INFO, "*Reintento* - {0} Eventos pendientes {1} recibidos correctamante. SmartDriver: {2}", new Object[]{events.length, type.name(), ll.getPerson().getEmail()});
                        pendingVehicleLocations.clear();
                        break;
                    case NORMAL_VEHICLE_LOCATION:
                        SimulatorController.increaseOkSends();
                        LOG.log(Level.FINE, "onCompletion() - Localización de trayecto simulado recibida correctamante. SmartDriver: {0}", ll.getPerson().getEmail());
                        locationChanged = false;
                        break;
                    case NORMAL_DATA_SECTION:
                        SimulatorController.increaseOkSends();
                        LOG.log(Level.FINE, "onCompletion() - Datos de sección de trayecto simulado recibidos correctamante. SmartDriver: {0}", ll.getPerson().getEmail());
                        break;
                    default:
                        break;
                }
            } else {
                LOG.log(Level.SEVERE, "onCompletion() - No se ha podido enviar a Kafka", exception);
                SimulatorController.logCurrentStatus();
                switch (type) {
                    case RECOVERED_VEHICLE_LOCATION:
                        if (retries != -1) {
                            // Los elementos ya están en la lista de pendientes, le restamos un reintento.
                            decreasePendingVehicleLocationsRetries();
                        }
                        break;
                    case RECOVERED_DATA_SECTION:
                        if (retries != -1) {
                            // Los elementos ya están en la lista de pendientes, le restamos un reintento.
                            decreasePendingDataSectionsRetries();
                        }
                        break;
                    case NORMAL_VEHICLE_LOCATION:
                        SimulatorController.increaseErrors();
                        if (SimulatorController.retryOnFail) {
                            // Si ha fallado, almacenamos el 'Vehicle Location' que se debería haber enviado y lo intentamos luego.
                            pendingVehicleLocations.addAll(Arrays.asList(events));
                        }
                        break;
                    case NORMAL_DATA_SECTION:
                        SimulatorController.increaseErrors();
                        if (SimulatorController.retryOnFail) {
                            // Si ha fallado, almacenamos el 'Vehicle Location' que se debería haber enviado y lo intentamos luego.
                            pendingDataSections.addAll(Arrays.asList(events));
                        }
                        break;
                    default:
                        break;
                }
            }
        }
    }
}
