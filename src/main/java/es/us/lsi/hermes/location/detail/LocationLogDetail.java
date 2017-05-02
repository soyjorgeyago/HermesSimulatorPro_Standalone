package es.us.lsi.hermes.location.detail;

import es.us.lsi.hermes.location.LocationLog;
import es.us.lsi.hermes.util.Constants;
import java.io.Serializable;
import java.util.ResourceBundle;

public class LocationLogDetail implements Serializable {

    private static final long serialVersionUID = 1L;
    private Integer locationLogDetailId;
    private double latitude;
    private double longitude;
    private double speed;
    private int heartRate;
    private int rrTime;
    private LocationLog locationLog;
    private int secondsToBeHere;

    public LocationLogDetail() {
        this.latitude = 0.0d;
        this.longitude = 0.0d;
        this.speed = 0.0d;
        this.heartRate = 0;
        this.rrTime = 0;
        this.secondsToBeHere = 0;
    }

    public LocationLogDetail(Integer locationLogDetailId) {
        this.locationLogDetailId = locationLogDetailId;
    }

    public LocationLogDetail(double latitude, double longitude, double speed, int heartRate, int rrTime, int secondsToBeHere) {
        this.latitude = latitude;
        this.longitude = longitude;
        this.speed = speed;
        this.heartRate = heartRate;
        this.rrTime = rrTime;
        this.secondsToBeHere = secondsToBeHere;
    }

    public Integer getLocationLogDetailId() {
        return locationLogDetailId;
    }

    public void setLocationLogDetailId(Integer locationLogDetailId) {
        this.locationLogDetailId = locationLogDetailId;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public double getSpeed() {
        return speed;
    }

    public void setSpeed(double speed) {
        this.speed = speed;
    }

    public int getHeartRate() {
        return heartRate;
    }

    public void setHeartRate(int heartRate) {
        this.heartRate = heartRate;
    }

    public LocationLog getLocationLog() {
        return locationLog;
    }

    public void setLocationLog(LocationLog locationLog) {
        this.locationLog = locationLog;
    }

    public int getRrTime() {
        return rrTime;
    }

    public void setRrTime(int rrTime) {
        this.rrTime = rrTime;
    }

    public int getSecondsToBeHere() {
        return secondsToBeHere;
    }

    public void setSecondsToBeHere(int secondsToBeHere) {
        this.secondsToBeHere = secondsToBeHere;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("[")
                .append(this.latitude)
                .append(", ")
                .append(this.longitude)
                .append(" -> HR: ")
                .append(this.heartRate)
                .append(", S: ")
                .append(this.speed)
                .append(" km/h]");

        return sb.toString();
    }

    public String getMarkerTitle() {
        StringBuilder sb = new StringBuilder();
        sb.append(ResourceBundle.getBundle("/Bundle").getString("Time")).append(": ").append(Constants.dfTime.format(System.currentTimeMillis() + (secondsToBeHere * 1000)));
        sb.append(" ");
        sb.append(ResourceBundle.getBundle("/Bundle").getString("HeartRate")).append(": ").append(Integer.toString(getHeartRate()));
        sb.append(" ");
        sb.append(ResourceBundle.getBundle("/Bundle").getString("Speed")).append(": ").append(Constants.df2Decimals.format(getSpeed())).append(" Km/h");
        sb.append(" (").append(getLatitude()).append(", ").append(getLongitude()).append(")");

        return sb.toString();
    }
}
