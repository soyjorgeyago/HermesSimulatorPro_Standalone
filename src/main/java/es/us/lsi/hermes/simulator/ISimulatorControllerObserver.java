package es.us.lsi.hermes.simulator;

import es.us.lsi.hermes.analysis.Vehicle;

public interface ISimulatorControllerObserver {
    public void update(Vehicle v);
}
