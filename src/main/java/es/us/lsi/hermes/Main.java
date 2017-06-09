package es.us.lsi.hermes;

import es.us.lsi.hermes.simulator.SimulatorController;

public class Main {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        boolean localMode = false;

        if (args.length > 0 && args[0].equalsIgnoreCase("LOCAL")) {
            localMode = true;
        }
        SimulatorController sc = new SimulatorController(localMode);
        sc.simulate();
    }
}
