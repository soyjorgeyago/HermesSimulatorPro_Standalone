package es.us.lsi.hermes.location;

import es.us.lsi.hermes.location.detail.LocationLogDetail;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class LocationLog implements Serializable {

    private static final long serialVersionUID = 1L;
    private Integer locationLogId;
    private List<LocationLogDetail> locationLogDetailList;
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
