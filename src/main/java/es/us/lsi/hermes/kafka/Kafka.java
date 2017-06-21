package es.us.lsi.hermes.kafka;

import es.us.lsi.hermes.util.Utils;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Kafka {

    private static final Logger LOG = Logger.getLogger(Kafka.class.getName());

    public static final String TOPIC_VEHICLE_LOCATION = "VehicleLocation";
    public static final String TOPIC_SIMULATOR_STATUS = "SimulatorStatus";

    private static final Properties KAFKA_PRODUCER_PROPERTIES;
    private static final Properties KAFKA_CONSUMER_PROPERTIES;
    private static final Properties KAFKA_MONITORING_PRODUCER_PROPERTIES;

    static {
        LOG.log(Level.INFO, "Kafka() - Kafka init.");

        KAFKA_PRODUCER_PROPERTIES = Utils.initProperties("KafkaProducer.properties");
        KAFKA_CONSUMER_PROPERTIES = Utils.initProperties("KafkaConsumer.properties");
        KAFKA_MONITORING_PRODUCER_PROPERTIES = Utils.initProperties("KafkaMonitoringProducer.properties");
    }

    public static Properties getKafkaProducerProperties() {
        return KAFKA_PRODUCER_PROPERTIES;
    }

    public static Properties getKafkaConsumerProperties() {
        return KAFKA_CONSUMER_PROPERTIES;
    }
    
    public static Properties getKafkaMonitoringProducerProperties() {
        return KAFKA_MONITORING_PRODUCER_PROPERTIES;
    }
}
