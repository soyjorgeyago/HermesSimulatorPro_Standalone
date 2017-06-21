package es.us.lsi.hermes.util.classes;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PathRequestWebService {

    private static final Logger LOG = Logger.getLogger(PathRequestWebService.class.getName());

    private static ThreadPoolExecutor executor;
    private static ThreadPoolMonitor monitor;

    public static synchronized void submitTask(Runnable runnable) {
        init();
        executor.submit(runnable);
    }

    public static synchronized Future<String> submitTask(Callable<String> callable) {
        init();
        return executor.submit(callable);
    }

    public static synchronized List<Future<String>> submitAllTask(List<Callable<String>> callableList) throws InterruptedException {
        init();
        return executor.invokeAll(callableList);
    }

    private static void init() {
        LOG.log(Level.INFO, "init() - Petición de rutas");
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
