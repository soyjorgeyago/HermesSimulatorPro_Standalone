package es.us.lsi.hermes.util;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.logging.Logger;
import javax.ejb.Singleton;
import javax.ejb.Startup;
import javax.inject.Named;

@Named(value = "constants")
@Singleton
@Startup
public class Constants {

    private static final Logger LOG = Logger.getLogger(Constants.class.getName());

    private static Constants instance;

    public static final SimpleDateFormat df = new SimpleDateFormat("dd/MM/yyyy");
    public static final SimpleDateFormat dfTime = new SimpleDateFormat("HH:mm:ss");
    public static final SimpleDateFormat dfISO8601 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    public static final DecimalFormat df2Decimals = new DecimalFormat("0.00");
    public static final SimpleDateFormat dfFile = new SimpleDateFormat("yyyy-MM-dd_HH.mm.ss");

    public static final String SIMULATOR_APPLICATION_ID = "SmartDriver";

    public static Constants getInstance() {
        return instance;
    }

}
