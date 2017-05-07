package es.us.lsi.hermes.simulator;

import es.us.lsi.hermes.analysis.Vehicle;

public interface ISimulatorControllerObserver {
    void update(Vehicle v);
}
