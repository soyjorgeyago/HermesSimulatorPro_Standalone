package es.us.lsi.hermes.topics;

import com.google.gson.annotations.Expose;

import java.io.Serializable;

public class VehicleLocation implements Serializable {

    @Expose
    private String vehicleId;
    @Expose
    private String timeStamp;
    @Expose
    private Double longitude;
    @Expose
    private Double latitude;
    @Expose
    private Integer stress;
    @Expose
    private int speed;
    @Expose
    private Integer accuracy;
    private int retries;

    public String getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(String timeStamp) {
        this.timeStamp = timeStamp;
    }

    public Double getLongitude() {
        return longitude;
    }

    public void setLongitude(Double longitude) {
        this.longitude = longitude;
    }

    public Integer getStress() {
        return stress;
    }

    public void setStress(Integer stress) {
        this.stress = stress;
    }

    public Double getLatitude() {
        return latitude;
    }

    public void setLatitude(Double latitude) {
        this.latitude = latitude;
    }

    public int getSpeed() {
        return speed;
    }

    public void setSpeed(int speed) {
        this.speed = speed;
    }

    public Integer getAccuracy() {
        return accuracy;
    }

    public void setAccuracy(Integer accuracy) {
        this.accuracy = accuracy;
    }

    public int getRetries() {
        return retries;
    }

    public void decreaseRetries() {
        retries--;
    }

    public void setVehicleId(String vehicleId) {
        this.vehicleId = vehicleId;
    }

    @Override
    public String toString() {
        return "VehicleLocation{" +
                "vehicleId='" + vehicleId + '\'' +
                ", timeStamp='" + timeStamp + '\'' +
                ", longitude=" + longitude +
                ", latitude=" + latitude +
                ", stress=" + stress +
                ", speed=" + speed +
                ", accuracy=" + accuracy +
                ", retries=" + retries +
                '}';
    }
}
