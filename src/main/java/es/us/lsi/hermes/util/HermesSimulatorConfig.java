package es.us.lsi.hermes.util;

import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class HermesSimulatorConfig {

    private static final Logger LOG = Logger.getLogger(HermesSimulatorConfig.class.getName());

    private static final Properties HERMES_SIMULATOR_PROPERTIES;

    static {
        LOG.log(Level.INFO, "HermesSimulatorConfig() - Hermes simulator init.");

        HERMES_SIMULATOR_PROPERTIES = Util.initProperties("HermesSimulator.properties");
    }

    public static Properties getHermesSimulatorProperties() {
        return HERMES_SIMULATOR_PROPERTIES;
    }
}
