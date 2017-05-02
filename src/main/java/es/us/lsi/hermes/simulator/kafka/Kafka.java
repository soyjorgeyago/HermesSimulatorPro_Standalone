package es.us.lsi.hermes.simulator.kafka;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.PostConstruct;
import javax.ejb.Singleton;
import javax.ejb.Startup;

@Singleton
@Startup
public class Kafka {

    private static final Logger LOG = Logger.getLogger(Kafka.class.getName());

    public static final String TOPIC_VEHICLE_LOCATION = "VehicleLocation";
    public static final String TOPIC_DATA_SECTION = "DataSection";

    private static Properties kafkaProducerProperties;
    private static Properties kafkaConsumerProperties;

    @PostConstruct
    public void onStartup() {
        LOG.log(Level.INFO, "onStartup() - Inicialización de Kafka");

        try {
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            InputStream input = classLoader.getResourceAsStream("KafkaProducer.properties");
            kafkaProducerProperties = new Properties();
            kafkaProducerProperties.load(input);
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, "onStartup() - Error al cargar el archivo de propiedades del 'producer' Kafka (KafkaProducer.properties)", ex);
        }
        
        try {
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            InputStream input = classLoader.getResourceAsStream("KafkaConsumer.properties");
            kafkaConsumerProperties = new Properties();
            kafkaConsumerProperties.load(input);
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, "onStartup() - Error al cargar el archivo de propiedades del 'consumer' de Kafka (KafkaConsumer.properties)", ex);
        }
    }

    public static Properties getKafkaProducerProperties() {
        return kafkaProducerProperties;
    }
    
    public static Properties getKafkaConsumerProperties() {
        return kafkaConsumerProperties;
    }
}
