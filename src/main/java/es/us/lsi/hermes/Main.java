package es.us.lsi.hermes;

import es.us.lsi.hermes.simulator.SimulatorController;

public class Main {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {

        SimulatorController sc = new SimulatorController();
        sc.simulate();
    }
}
