package es.us.lsi.hermes.google.directions;

public class Step {

    private Distance distance;
    private Duration duration;
    private Location endLocation;
    private Polyline polyline;
    private Location startLocation;

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
     * @return The polyline
     */
    public Polyline getPolyline() {
        return polyline;
    }

    /**
     *
     * @param polyline The polyline
     */
    public void setPolyline(Polyline polyline) {
        this.polyline = polyline;
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

}
