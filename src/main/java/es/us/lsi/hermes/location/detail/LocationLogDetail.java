package es.us.lsi.hermes.location.detail;

import es.us.lsi.hermes.csv.ICSVBean;
import es.us.lsi.hermes.util.Constants;
import org.supercsv.cellprocessor.ParseDouble;
import org.supercsv.cellprocessor.ParseInt;
import org.supercsv.cellprocessor.ift.CellProcessor;

import java.io.Serializable;

public class LocationLogDetail implements Serializable, ICSVBean {

    private static final long serialVersionUID = 1L;
    private double latitude;
    private double longitude;
    private int speed;
    private int heartRate;
    private int rrTime;
    private int secondsToRemainHere;

    public LocationLogDetail() {
        this.latitude = 0.0d;
        this.longitude = 0.0d;
        this.speed = 0;
        this.heartRate = 0;
        this.rrTime = 0;
        this.secondsToRemainHere = 0;
        initCSV();
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
        this.secondsToRemainHere = secondsToBeHere;
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

    public int getSecondsToRemainHere() {
        return secondsToRemainHere;
    }

    public void setSecondsToRemainHere(int secondsToRemainHere) {
        this.secondsToRemainHere = secondsToRemainHere;
    }

    @Override
    public String toString() {
        return "[" + this.latitude + ", " + this.longitude + " -> HR: " + this.heartRate + ", S: " + this.speed + " km/h]";
    }

//    public String getMarkerTitle() {
//        StringBuilder sb = new StringBuilder();
//        sb.append(ResourceBundle.getBundle("/Bundle").getString("Time")).append(": ").append(Constants.dfTime.format(System.currentTimeMillis() + (secondsToRemainHere * 1000)));
//        sb.append(" ");
//        sb.append(ResourceBundle.getBundle("/Bundle").getString("HeartRate")).append(": ").append(Integer.toString(getHeartRate()));
//        sb.append(" ");
//        sb.append(ResourceBundle.getBundle("/Bundle").getString("Speed")).append(": ").append(Constants.df2Decimals.format(getSpeed())).append(" Km/h");
//        sb.append(" (").append(getLatitude()).append(", ").append(getLongitude()).append(")");
//
//        return sb.toString();
//    }
    // ------------------------- CSV IMP/EXP -------------------------
    private CellProcessor[] cellProcessors;
    private String[] fields;
    private String[] headers;

    @Override
    public void initCSV() {
        cellProcessors = new CellProcessor[]{new ParseDouble(), new ParseDouble(), new ParseInt(), new ParseInt(), new ParseInt()};

        headers = new String[]{"Latitude", "Longitude", "Speed", "SecondsToRemainHere", "RrTime"};

        fields = new String[]{"latitude", "longitude", "speed", "secondsToRemainHere", "rrTime"};
    }

    @Override
    public CellProcessor[] getProcessors() {
        return cellProcessors;
    }

    @Override
    public String[] getFields() {
        return fields;
    }

    @Override
    public String[] getHeaders() {
        return headers;
    }
}
