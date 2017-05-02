package es.us.lsi.hermes.simulator;

import com.google.gson.Gson;
import es.us.lsi.hermes.analysis.Vehicle;
import es.us.lsi.hermes.simulator.kafka.Kafka;
import es.us.lsi.hermes.util.Constants;
import java.util.Collections;
import java.util.logging.Level;
import java.util.logging.Logger;
import kafka.utils.ShutdownableThread;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class SurroundingVehiclesConsumer extends ShutdownableThread {

    private static final Logger LOG = Logger.getLogger(SurroundingVehiclesConsumer.class.getName());
    public static final String TOPIC_SURROUNDING_VEHICLES = "SurroundingVehicles";

    private final KafkaConsumer<String, String> kafkaConsumer;
    private final long pollTimeout;
//    private final String sourceId;
    private final ISimulatorControllerObserver observer;
    private final Gson gson;

//    public SurroundingVehiclesConsumer(long pollTimeout, String sourceId, ISimulatedSmartDriverObserver observer) {
    public SurroundingVehiclesConsumer(ISimulatorControllerObserver observer) {
        // Podrá ser interrumpible.
        super("SurroundingVehiclesConsumer", true);
        // TODO: Investigar si mediante el consumer.id o mediante el group.id podemos hacer que cada consumer coja lo suyo únicamente.
//        props.put("consumer.id", sourceId);
//        this.sourceId = sourceId;
        this.kafkaConsumer = new KafkaConsumer<>(Kafka.getKafkaConsumerProperties());
        this.pollTimeout = Long.parseLong(Kafka.getKafkaConsumerProperties().getProperty("consumer.poll.timeout.ms", "1000"));
        this.observer = observer;
        this.gson = new Gson();
    }

    public void stopConsumer() {
        kafkaConsumer.close();
        shutdown();
    }

    @Override
    public void doWork() {
//        consumer.subscribe(Collections.singletonList(TOPIC_SURROUNDING_VEHICLES + sourceId));
        kafkaConsumer.subscribe(Collections.singletonList(TOPIC_SURROUNDING_VEHICLES));
        ConsumerRecords<String, String> records = kafkaConsumer.poll(pollTimeout);
        for (ConsumerRecord<String, String> record : records) {
            LOG.log(Level.FINE, "SurroundingVehiclesConsumer.doWork() - {0}: {1} [{2}] con offset {3}", new Object[]{record.topic(), Constants.dfISO8601.format(record.timestamp()), record.key(), record.offset()});

            Vehicle vehicle = gson.fromJson(record.value(), Vehicle.class);
            observer.update(vehicle);
        }
    }
}
