package es.us.lsi.hermes.util.classes;

//import es.us.lsi.hermes.analysis.Vehicle;

public interface ISimulatorControllerObserver {
//    void update(Vehicle v);

    void update(String id, int surroundingSize);
}
