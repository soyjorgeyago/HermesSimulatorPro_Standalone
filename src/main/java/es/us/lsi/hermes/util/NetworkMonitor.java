package es.us.lsi.hermes.util;

import javax.swing.*;
import java.awt.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.StringTokenizer;

public class NetworkMonitor extends JPanel implements Runnable {

    private ArrayList<String> lines;

    public NetworkMonitor() {
        lines = new ArrayList<>();
        new Thread(this).start();
    }

    @Override
    public void run() {
        try {
            while (true) {
                String sContents = readFile();
                StringTokenizer tok = new StringTokenizer(sContents, "\n");
                lines = new ArrayList<String>();
                int x = 0;

                while (tok.hasMoreTokens()) {
                    String line = tok.nextToken();
                    if (x++ < 2) {//skip headers
                        continue;
                    }
                    lines.add(line);
                }
                repaint();
                Thread.sleep(500);//half a second
            }
        } catch (Exception exc) {
            exc.printStackTrace();
        }
    }

    @Override
    public void paint(Graphics gr) {
        super.paint(gr);
        Graphics2D g = (Graphics2D) gr;
        g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
        g.setFont(new Font(Font.DIALOG_INPUT, Font.PLAIN, 14));
        g.drawString("Interface: receivedBytes/receivedPackets, sentBytes/sentPackets", 10, 20);
        for (int i = 0; i < lines.size(); i++) {
            drawLine(g, lines.get(i), i);
        }
    }

    private static final String readFile() {
        try {
            File file = new File("/proc/net/dev");
            StringBuilder sb = new StringBuilder();
            byte[] bytes = new byte[1024];
            int read;
            InputStream in = new FileInputStream(file);
            while ((read = in.read(bytes)) != -1) {
                sb.append(new String(bytes, 0, read));
            }
            in.close();
            return sb.toString();
        } catch (Exception exc) {
            exc.printStackTrace();
        }
        return null;
    }

    private void drawLine(Graphics2D g, String line, int iLineIndex) {
        String[] words = line.split("\\s+");
        String sInterface = words[words[0].length() > 0 ? 0 : 1];
        int index = sInterface.indexOf(':');
        boolean jump = index != sInterface.length()-1;
        String receivedBytes = jump ? sInterface.substring(index+1) : words[2];
        String receivedPackets = jump ? words[2] : words[3];
        String sentBytes = jump ? words[9] : words[10];
        String sentPackets = jump ? words[10] : words[11];
        if(index > -1) {
            sInterface = sInterface.substring(0, index);
        }
        int x = 10, y = 50 + iLineIndex * 20;
        String sReceived = receivedBytes + "/" + receivedPackets;
        String sSent = sentBytes + "/" + sentPackets;
        g.drawString(sInterface + ":", x, y);
        g.drawString(sReceived, x+100, y);
        g.drawString(sSent, x+250, y);
    }
}