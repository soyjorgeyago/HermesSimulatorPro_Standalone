package es.us.lsi.hermes.simulator.kafka;

import es.us.lsi.hermes.util.Util;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Kafka {

    private static final Logger LOG = Logger.getLogger(Kafka.class.getName());

    public static final String TOPIC_VEHICLE_LOCATION = "VehicleLocation";
    public static final String TOPIC_DATA_SECTION = "DataSection";

    private static Properties kafkaProducerProperties;
    private static Properties kafkaConsumerProperties;

    static {
        LOG.log(Level.INFO, "onStartup() - Inicialización de Kafka");

        kafkaProducerProperties = Util.initProperties("KafkaProducer.properties", LOG);

        kafkaConsumerProperties = Util.initProperties("KafkaConsumer.properties", LOG);
    }

    public static Properties getKafkaProducerProperties() {
        return kafkaProducerProperties;
    }

    public static Properties getKafkaConsumerProperties() {
        return kafkaConsumerProperties;
    }
}
