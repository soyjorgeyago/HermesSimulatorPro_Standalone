package es.us.lsi.hermes.simulator;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ThreadPoolMonitor implements Runnable {

    private static final Logger LOG = Logger.getLogger(ThreadPoolMonitor.class.getName());

    private final ThreadPoolExecutor executor;
    private final int seconds;
    private boolean run = true;

    public ThreadPoolMonitor(ThreadPoolExecutor executor, int delay) {
        this.executor = executor;
        this.seconds = delay;
    }

    public void shutdown() {
        this.run = false;
    }

    @Override
    public void run() {
        while (run) {
            LOG.log(Level.FINE, String.format("ThreadPoolMonitor [Pool size:%d / Core: %d] Active: %d, Completed: %d, Task: %d, isShutdown: %s, isTerminated: %s",
                    this.executor.getPoolSize(),
                    this.executor.getCorePoolSize(),
                    this.executor.getActiveCount(),
                    this.executor.getCompletedTaskCount(),
                    this.executor.getTaskCount(),
                    this.executor.isShutdown(),
                    this.executor.isTerminated()));
            try {
                Thread.sleep(seconds * 1000);
            } catch (InterruptedException ex) {
                LOG.log(Level.SEVERE, "ThreadPoolMonitor - Error al monitorizar el 'pool' de tareas ", ex);
            }
        }
    }
}
