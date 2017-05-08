package es.us.lsi.hermes;

import es.us.lsi.hermes.simulator.SimulatorController;
import org.apache.commons.lang.SystemUtils;

import javax.swing.*;

public class Main extends JFrame {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {

        SimulatorController sc = new SimulatorController();
        sc.simulate();
    }
}
