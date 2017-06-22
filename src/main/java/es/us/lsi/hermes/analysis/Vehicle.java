package es.us.lsi.hermes.analysis;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

public class Vehicle implements Serializable {

    private final String id;
    private double latitude, longitude;
    private int speed;
    private int stress;
    private final Set<String> surroundingVehicles;
    private long lastUpdate;

    /**
     * Constructor en el que se indicará el identificador del 'SmartDriver'.
     *
     * @param id identificador del 'SmartDriver'
     */
    public Vehicle(String id) {
        this.id = id;
        this.stress = 0;
        this.speed = 0;
        this.latitude = 0.0d;
        this.longitude = 0.0d;
        this.surroundingVehicles = new HashSet<>();
        this.lastUpdate = System.currentTimeMillis();
    }

    public String getId() {
        return id;
    }

    public double getLatitude() {
        return latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public int getSpeed() {
        return speed;
    }

    public int getStress() {
        return stress;
    }

    public Set<String> getSurroundingVehicles() {
        return surroundingVehicles;
    }

    public long getLastUpdate() {
        return lastUpdate;
    }
}