package es.us.lsi.hermes;

import es.us.lsi.hermes.simulator.SimulatorController;
import es.us.lsi.hermes.util.NetworkMonitor;
import org.apache.commons.lang.SystemUtils;

import javax.swing.*;

public class Main extends JFrame {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {

        if(SystemUtils.IS_OS_LINUX)
            new Main();

        SimulatorController sc = new SimulatorController();
        sc.simulate();
    }

    // Network Monitor interface
    private Main() {
        setSize(550, 200);
        setTitle("Network Monitor");
        setContentPane(new NetworkMonitor());
        setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
        setVisible(true);
    }
}
