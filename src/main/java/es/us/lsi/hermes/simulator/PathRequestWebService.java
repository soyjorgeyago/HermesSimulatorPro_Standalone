package es.us.lsi.hermes.simulator;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;

@WebListener
public class PathRequestWebService implements ServletContextListener {

    private static final Logger LOG = Logger.getLogger(PathRequestWebService.class.getName());

    private static ThreadPoolExecutor executor;
    private static ThreadPoolMonitor monitor;

    @Override
    public void contextInitialized(ServletContextEvent sce) {
        createExecutor();
        LOG.log(Level.INFO, "contextInitialized() - Inicialización");
    }

    @Override
    public void contextDestroyed(ServletContextEvent sce) {
        executor.shutdown();
        LOG.log(Level.INFO, "contextDestroyed() - Finalización");
    }

    public static synchronized void submitTask(Runnable runnable) {
        init();
        executor.submit(runnable);
    }

    public static synchronized Future<String> submitTask(Callable callable) {
        init();
        return executor.submit(callable);
    }

    public static synchronized List<Future<String>> submitAllTask(List<Callable<String>> callableList) throws InterruptedException {
        init();
        return executor.invokeAll(callableList);
    }

    private static void init() {
        shutdown();
        createExecutor();
        monitor = new ThreadPoolMonitor(executor, 1);
        Thread monitorThread = new Thread(monitor);
        monitorThread.start();
    }

    public static synchronized void shutdown() {
        if (executor != null) {
            executor.shutdown();
        }
        if (monitor != null) {
            monitor.shutdown();
        }
    }

    private static void createExecutor() {
        executor = new ThreadPoolExecutor(
                100, // Tendremos siempre 100 hilos activos.
                100, // Número máximo de hilos (pool).
                60L, // Tiempo máximo que esperarán los nuevos hilos que lleguen, si todos los hilos del 'pool' están ocupados.
                TimeUnit.SECONDS, // Unidad de medida para el tiempo de espera máximo.
                new LinkedBlockingQueue<Runnable>()); // La cola que se usará para almacenar los hilos antes de ser ejecutados, para resolver el problema productor-consumidor a distintas velocidades.
    }
}
