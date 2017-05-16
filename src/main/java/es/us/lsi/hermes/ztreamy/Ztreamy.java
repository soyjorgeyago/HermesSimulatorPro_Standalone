package es.us.lsi.hermes.ztreamy;

import es.us.lsi.hermes.util.Util;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Ztreamy {

    private static final Logger LOG = Logger.getLogger(Ztreamy.class.getName());
    private static final Properties ZTREAMY_PROPERTIES;

    static {
        LOG.log(Level.INFO, "Ztreamy() - Ztreamy init.");

        ZTREAMY_PROPERTIES = Util.initProperties("Ztreamy.properties");
    }

    public static URL getServerUrl() {
        URL url = null;

        try {
            url = new URL(ZTREAMY_PROPERTIES.getProperty("ztreamy.url", "http://hermes1.gast.it.uc3m.es:9220/collector/publish"));
        } catch (MalformedURLException ex) {
            LOG.log(Level.SEVERE, "getServerUrl() - Unable to create Ztreamy server URL", ex);
        }

        return url;
    }
}
