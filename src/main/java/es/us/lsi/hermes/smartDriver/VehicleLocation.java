package es.us.lsi.hermes.smartDriver;

import java.io.Serializable;

public class VehicleLocation implements Serializable {

    private String timeStamp;
    private Double longitude;
    private Integer stress;
    private Double latitude;
    private Double speed;
    private Integer accuracy;

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

    public Double getSpeed() {
        return speed;
    }

    public void setSpeed(Double speed) {
        this.speed = speed;
    }

    public Integer getAccuracy() {
        return accuracy;
    }

    public void setAccuracy(Integer accuracy) {
        this.accuracy = accuracy;
    }


    @Override
    public String toString() {
        return "VehicleLocation{" +
                "timeStamp='" + timeStamp + '\'' +
                ", longitude=" + longitude +
                ", stress=" + stress +
                ", latitude=" + latitude +
                ", speed=" + speed +
                ", accuracy=" + accuracy +
                '}';
    }
}
