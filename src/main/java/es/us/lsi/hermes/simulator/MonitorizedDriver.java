package es.us.lsi.hermes.simulator;

public abstract class MonitorizedDriver {

    private int generated;
    private int sent;
    private int oks;
    private int notOks;
    private int errors;
    private int recovered;

    private int elapsedSeconds;
    private int pointToPointElapsedSeconds;
    private long currentDelayMs;

    private int currentPosition;

    private boolean finished;

    public MonitorizedDriver() {
        this.generated = 0;
        this.sent = 0;
        this.oks = 0;
        this.notOks = 0;
        this.errors = 0;
        this.recovered = 0;
        this.elapsedSeconds = 0;
        this.pointToPointElapsedSeconds = 0;
        this.currentDelayMs = 0l;
        this.currentPosition = 0;
        this.finished = false;
    }

    public int getGenerated() {
        return generated;
    }

    public void increaseGenerated() {
        this.generated++;
    }

    public int getSent() {
        return sent;
    }

    public void increaseSent() {
        this.sent++;
    }

    public int getOks() {
        return oks;
    }

    public void increaseOks() {
        this.oks++;
    }

    public int getNotOks() {
        return notOks;
    }

    public void increaseNotOks() {
        this.notOks++;
    }

    public int getErrors() {
        return errors;
    }

    public void increaseErrors() {
        this.errors++;
    }

    public int getRecovered() {
        return recovered;
    }

    public void addRecovered(int recovered) {
        this.recovered += recovered;
    }

    public int getDriverSimulationTimeInSeconds() {
        return elapsedSeconds;
    }
    
    public int getPointToPointElapsedSeconds() {
        return pointToPointElapsedSeconds;
    }

    public void increaseDriverSimulationTime() {
        this.elapsedSeconds++;
        this.pointToPointElapsedSeconds++;
    }

    public void resetPointToPointElapsedSeconds() {
        this.pointToPointElapsedSeconds = 0;
    }

    public long getCurrentDelayMs() {
        return currentDelayMs;
    }

    public void setCurrentDelayMs(long currentDelayMs) {
        this.currentDelayMs = currentDelayMs;
    }

    public int getCurrentPosition() {
        return currentPosition;
    }

    public void setCurrentPosition(int currentPosition) {
        this.currentPosition = currentPosition;
    }

    public boolean hasFinished() {
        return finished;
    }

    public void setFinished() {
        this.finished = true;
    }

    public abstract int getPending();
}
