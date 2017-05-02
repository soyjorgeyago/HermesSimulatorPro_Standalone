package es.us.lsi.hermes.analysis;

import es.us.lsi.hermes.smartDriver.Location;
import java.util.List;

public class Vehicle {

    private String id;
    private int score;
    private List<Location> historicLocations;
    private List<SurroundingVehicle> surroundingVehicles;

    public Vehicle() {
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public int getScore() {
        return score;
    }

    public void setScore(int score) {
        this.score = score;
    }

    public List<Location> getHistoricLocations() {
        return historicLocations;
    }

    public void setHistoricLocations(List<Location> historicLocations) {
        this.historicLocations = historicLocations;
    }

    public List<SurroundingVehicle> getSurroundingVehicles() {
        return surroundingVehicles;
    }

    public void setSurroundingVehicles(List<SurroundingVehicle> surroundingVehicles) {
        this.surroundingVehicles = surroundingVehicles;
    }

    public class SurroundingVehicle {

        private String id;
        private int score;
        private Double latitude;
        private Double longitude;

        public SurroundingVehicle() {
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public int getScore() {
            return score;
        }

        public void setScore(int score) {
            this.score = score;
        }

        public Double getLatitude() {
            return latitude;
        }

        public void setLatitude(Double latitude) {
            this.latitude = latitude;
        }

        public Double getLongitude() {
            return longitude;
        }

        public void setLongitude(Double longitude) {
            this.longitude = longitude;
        }
    }
}
