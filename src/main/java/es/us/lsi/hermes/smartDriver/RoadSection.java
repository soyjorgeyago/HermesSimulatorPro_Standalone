package es.us.lsi.hermes.smartDriver;

import es.us.lsi.hermes.util.Constants;
import java.io.Serializable;

public class RoadSection implements Serializable {

    private transient long time; // Para que no la serialice el GSON.
    private String timeStamp;
    private Double latitude;
    private Double longitude;
    private Double speed;
    private Integer accuracy;
    private transient int heartRate; // Para que no la serialice el GSON.
    private transient int rrTime; // Para que no la serialice el GSON.

    public Double getLatitude() {
        return latitude;
    }

    public void setLatitude(Double latitude) {
        this.latitude = latitude;
    }

    public String getTimeStamp() {
        return timeStamp;
    }

    public void setTime(long time) {
        this.time = time;
        this.timeStamp = Constants.dfISO8601.format(time);
    }

    public long getTime() {
        return time;
    }

    public Double getSpeed() {
        return speed;
    }

    public void setSpeed(Double speed) {
        this.speed = speed;
    }

    public Double getLongitude() {
        return longitude;
    }

    public void setLongitude(Double longitude) {
        this.longitude = longitude;
    }

    public Integer getAccuracy() {
        return accuracy;
    }

    public void setAccuracy(Integer accuracy) {
        this.accuracy = accuracy;
    }

    public int getHeartRate() {
        return heartRate;
    }

    public void setHeartRate(int heartRate) {
        this.heartRate = heartRate;
    }

    public int getRrTime() {
        return rrTime;
    }

    public void setRrTime(int rrTime) {
        this.rrTime = rrTime;
    }
}
