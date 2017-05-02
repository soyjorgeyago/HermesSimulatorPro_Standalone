package es.us.lsi.hermes.location;

import es.us.lsi.hermes.location.detail.LocationLogDetail;
import es.us.lsi.hermes.person.Person;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class LocationLog implements Serializable {

    private static final long serialVersionUID = 1L;
    private Integer locationLogId;
    private Person person;
    private List<LocationLogDetail> locationLogDetailList;
    private String filename;

    private int duration;
    private double distance;

    public LocationLog() {
        locationLogDetailList = new ArrayList<>();
    }

    public Integer getLocationLogId() {
        return locationLogId;
    }

    public void setLocationLogId(Integer locationLogId) {
        this.locationLogId = locationLogId;
    }

    public Date getDateLog() {
        return new Date(System.currentTimeMillis());
    }

    public Person getPerson() {
        return person;
    }

    public void setPerson(Person person) {
        this.person = person;
    }

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public List<LocationLogDetail> getLocationLogDetailList() {
        return locationLogDetailList;
    }

    public void setLocationLogDetailList(List<LocationLogDetail> locationLogDetailList) {
        this.locationLogDetailList = locationLogDetailList;
    }

    public int getDuration() {
        return duration;
    }

    public void setDuration(int duration) {
        this.duration = duration;
    }

    public double getDistance() {
        return distance;
    }

    public void setDistance(double distance) {
        this.distance = distance;
    }
}
