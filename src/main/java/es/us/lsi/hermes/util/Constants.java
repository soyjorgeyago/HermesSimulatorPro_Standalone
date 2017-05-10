package es.us.lsi.hermes.util;

import es.us.lsi.hermes.google.directions.Location;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.logging.Logger;

public class Constants {

    private static final Logger LOG = Logger.getLogger(Constants.class.getName());

//  APP RELATED
    public static final String SIMULATOR_APPLICATION_ID = "SmartDriver";
    public static final String DEFAULT_EMAIL = "jorgeyago.ingeniero@gmail.com";

    public static final int ZIP_FILE_SIZE = 26214400;

//  FORMATTING
    public static final SimpleDateFormat df = new SimpleDateFormat("dd/MM/yyyy");
    public static final SimpleDateFormat dfTime = new SimpleDateFormat("HH:mm:ss");
    public static final SimpleDateFormat dfISO8601 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    public static final DecimalFormat df2Decimals = new DecimalFormat("0.00");
    public static final SimpleDateFormat dfFile = new SimpleDateFormat("yyyy-MM-dd_HH.mm.ss");

//  CONTROLLER
    // El 'dashboard' está en: http://hermes1.gast.it.uc3m.es:9209/backend/dashboard.html
    public static final String ZTREAMY_URL = HermesSimulatorConfig.getHermesSimulatorProperties().getProperty("ztreamy.url", "http://hermes1.gast.it.uc3m.es:9220/collector/publish"); // URL de Ztreamy OFICIAL

    public static final Location SEVILLE = new Location(37.3898358, -5.986069);

    public static final int RR_TIME = 850; // Equivale a una frecuencia cardíaca en reposo media (70 ppm).

    // Máximo retardo para iniciar la ruta, en milisegundos:
    public static final int MAX_INITIAL_DELAY = 60000;

    // Paquetes de peticiones en la generación de trayectos. Google ha limitado más el número de peticiones por segundo.
    public static final int REQUEST_PACK_SIZE = 10;

    // Número máximo de hilos en el simulador.
    public static final int MAX_THREADS = Integer.parseInt(HermesSimulatorConfig.getHermesSimulatorProperties().getProperty("max.threads", "10000"));
    // Tiempo máximo de simulación.
    // JYFR: PRUEBA
//    public static final long MAX_SIMULATION_TIME = Long.parseLong(HermesSimulatorConfig.getHermesSimulatorProperties().getProperty("max.simulation.time.ms", "14400000"));

    public static final int STATUS_SAMPLING_INTERVAL_S = Integer.parseInt(HermesSimulatorConfig.getHermesSimulatorProperties().getProperty("status.sampling.interval.s", "2"));

    public static final int MAX_ACCEPTABLE_DELAY_S = Integer.parseInt(HermesSimulatorConfig.getHermesSimulatorProperties().getProperty("max.acceptable.delay.s", "5"));

}
