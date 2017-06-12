package es.us.lsi.hermes.location.detail;

import es.us.lsi.hermes.csv.ICSVBean;
import org.supercsv.cellprocessor.ParseDouble;
import org.supercsv.cellprocessor.ParseInt;
import org.supercsv.cellprocessor.ift.CellProcessor;

import java.io.Serializable;

public class LocationLogDetail implements Serializable, ICSVBean {

    private static final long serialVersionUID = 1L;
    private double latitude;
    private double longitude;
    private int speed;
    private int secondsToRemainHere;

    public LocationLogDetail() {
        this.latitude = 0.0d;
        this.longitude = 0.0d;
        this.speed = 0;
        this.secondsToRemainHere = 0;
        initCSV();
    }

    public LocationLogDetail(double lat, double lng, int speed) {
        this.latitude = lat;
        this.longitude = lng;
        this.speed = speed;
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

    public int getSecondsToRemainHere() {
        return secondsToRemainHere;
    }

    public void setSecondsToRemainHere(int secondsToRemainHere) {
        this.secondsToRemainHere = secondsToRemainHere;
    }

    @Override
    public String toString() {
        return "[" + this.latitude + ", " + this.longitude + ", S: " + this.speed + " km/h]";
    }

    // ------------------------- CSV IMP/EXP -------------------------

    private CellProcessor[] cellProcessors;
    private String[] fields;
    private String[] headers;

    @Override
    public void initCSV() {
        cellProcessors = new CellProcessor[]{new ParseDouble(), new ParseDouble(), new ParseInt(), new ParseInt()};

        headers = new String[]{"Latitude", "Longitude", "Speed", "SecondsToRemainHere"};

        fields = new String[]{"latitude", "longitude", "speed", "secondsToRemainHere"};
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
