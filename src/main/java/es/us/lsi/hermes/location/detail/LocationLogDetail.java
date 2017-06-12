package es.us.lsi.hermes.location.detail;

import es.us.lsi.hermes.util.Constants;
import org.supercsv.cellprocessor.ParseDouble;
import org.supercsv.cellprocessor.ParseInt;
import org.supercsv.cellprocessor.ift.CellProcessor;

import java.io.Serializable;

public class LocationLogDetail implements Serializable {

    private static final long serialVersionUID = 1L;
    private double latitude;
    private double longitude;
    private int speed;
    private int heartRate;
    private int rrTime;
    private int secondsToBeHere;

    public LocationLogDetail() {
        this.latitude = 0.0d;
        this.longitude = 0.0d;
        this.speed = 0;
        this.heartRate = 0;
        this.rrTime = 0;
        this.secondsToBeHere = 0;
    }

    public LocationLogDetail(double lat, double lng, int speed) {
        this.latitude = lat;
        this.longitude = lng;
        this.rrTime = Constants.RR_TIME;
        this.heartRate = (int) Math.ceil(60.0d / (Constants.RR_TIME / 1000.0d));
        this.speed = speed;
    }

    public LocationLogDetail(double latitude, double longitude, int speed, int heartRate, int rrTime, int secondsToBeHere) {
        this.latitude = latitude;
        this.longitude = longitude;
        this.speed = speed;
        this.heartRate = heartRate;
        this.rrTime = rrTime;
        this.secondsToBeHere = secondsToBeHere;
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

    public int getSpeed() {
        return speed;
    }

    public void setSpeed(int speed) {
        this.speed = speed;
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

    public int getSecondsToBeHere() {
        return secondsToBeHere;
    }

    public void setSecondsToBeHere(int secondsToBeHere) {
        this.secondsToBeHere = secondsToBeHere;
    }

    @Override
    public String toString() {
        return "[" + this.latitude + ", " + this.longitude + " -> HR: " + this.heartRate + ", S: " + this.speed + " km/h]";
    }

//    public String getMarkerTitle() {
//        StringBuilder sb = new StringBuilder();
//        sb.append(ResourceBundle.getBundle("/Bundle").getString("Time")).append(": ").append(Constants.dfTime.format(System.currentTimeMillis() + (secondsToBeHere * 1000)));
//        sb.append(" ");
//        sb.append(ResourceBundle.getBundle("/Bundle").getString("HeartRate")).append(": ").append(Integer.toString(getHeartRate()));
//        sb.append(" ");
//        sb.append(ResourceBundle.getBundle("/Bundle").getString("Speed")).append(": ").append(Constants.df2Decimals.format(getSpeed())).append(" Km/h");
//        sb.append(" (").append(getLatitude()).append(", ").append(getLongitude()).append(")");
//
//        return sb.toString();
//    }
    // ------------------------- CSV IMP/EXP -------------------------
    private static CellProcessor[] cellProcessors = new CellProcessor[]{new ParseDouble(), new ParseDouble(), new ParseInt(), new ParseInt(), new ParseInt()};
    private static String[] fields = new String[]{"Latitude", "Longitude", "Speed", "SecondsToBeHere", "RrTime"};
    private static String[] headers = new String[]{"latitude", "longitude", "speed", "secondsToBeHere", "rrTime"};

    public static CellProcessor[] getProcessors() {
        return cellProcessors;
    }

    public static String[] getFields() {
        return fields;
    }

    public static String[] getHeaders() {
        return headers;
    }
}
