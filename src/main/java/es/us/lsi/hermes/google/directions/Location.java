package es.us.lsi.hermes.google.directions;

public class Location {

    private Double lat;
    private Double lng;

    public Location() {
    }

    public Location(Double lat, Double lng) {
        this.lat = lat;
        this.lng = lng;
    }

    /**
     *
     * @return The lat
     */
    public Double getLat() {
        return lat;
    }

    /**
     *
     * @param lat The lat
     */
    public void setLat(Double lat) {
        this.lat = lat;
    }

    /**
     *
     * @return The lng
     */
    public Double getLng() {
        return lng;
    }

    /**
     *
     * @param lng The lng
     */
    public void setLng(Double lng) {
        this.lng = lng;
    }

}
