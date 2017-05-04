package es.us.lsi.hermes.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class HermesSimulatorConfig {

    private static final Logger LOG = Logger.getLogger(HermesSimulatorConfig.class.getName());

    private static Properties hermesSimulatorProperties;

    static {
        LOG.log(Level.INFO, "init() - Inicialización del simulador");

        try {
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            InputStream input = classLoader.getResourceAsStream("HermesSimulator.properties");
            hermesSimulatorProperties = new Properties();
            hermesSimulatorProperties.load(input);
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, "init() - Error al cargar el archivo de propiedades del simulador (HermesSimulator.properties)", ex);
        }
    }

    public static Properties getHermesSimulatorProperties() {
        return hermesSimulatorProperties;
    }
}
