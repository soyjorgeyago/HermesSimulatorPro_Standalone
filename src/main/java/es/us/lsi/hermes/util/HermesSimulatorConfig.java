package es.us.lsi.hermes.util;

import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class HermesSimulatorConfig {

    private static final Logger LOG = Logger.getLogger(HermesSimulatorConfig.class.getName());

    private static Properties hermesSimulatorProperties;

    static {
        LOG.log(Level.INFO, "init() - Inicialización del simulador");

        hermesSimulatorProperties = Util.initProperties("HermesSimulator.properties", LOG);
    }

    public static Properties getHermesSimulatorProperties() {
        return hermesSimulatorProperties;
    }
}
