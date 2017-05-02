package es.us.lsi.hermes.google.directions;

import java.util.ArrayList;
import java.util.List;

public class Leg {

    private Distance distance;
    private Duration duration;
    private String endAddress;
    private Location endLocation;
    private String startAddress;
    private Location startLocation;
    private List<Step> steps = new ArrayList<>();

    /**
     *
     * @return The distance
     */
    public Distance getDistance() {
        return distance;
    }

    /**
     *
     * @param distance The distance
     */
    public void setDistance(Distance distance) {
        this.distance = distance;
    }

    /**
     *
     * @return The duration
     */
    public Duration getDuration() {
        return duration;
    }

    /**
     *
     * @param duration The duration
     */
    public void setDuration(Duration duration) {
        this.duration = duration;
    }

    /**
     *
     * @return The endAddress
     */
    public String getEndAddress() {
        return endAddress;
    }

    /**
     *
     * @param endAddress The end_address
     */
    public void setEndAddress(String endAddress) {
        this.endAddress = endAddress;
    }

    /**
     *
     * @return The endLocation
     */
    public Location getEndLocation() {
        return endLocation;
    }

    /**
     *
     * @param endLocation The end_location
     */
    public void setEndLocation(Location endLocation) {
        this.endLocation = endLocation;
    }

    /**
     *
     * @return The startAddress
     */
    public String getStartAddress() {
        return startAddress;
    }

    /**
     *
     * @param startAddress The start_address
     */
    public void setStartAddress(String startAddress) {
        this.startAddress = startAddress;
    }

    /**
     *
     * @return The startLocation
     */
    public Location getStartLocation() {
        return startLocation;
    }

    /**
     *
     * @param startLocation The start_location
     */
    public void setStartLocation(Location startLocation) {
        this.startLocation = startLocation;
    }

    /**
     *
     * @return The steps
     */
    public List<Step> getSteps() {
        return steps;
    }

    /**
     *
     * @param steps The steps
     */
    public void setSteps(List<Step> steps) {
        this.steps = steps;
    }

}
