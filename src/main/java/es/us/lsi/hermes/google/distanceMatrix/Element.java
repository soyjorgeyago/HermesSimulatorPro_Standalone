package es.us.lsi.hermes.google.distanceMatrix;

import es.us.lsi.hermes.google.directions.Distance;
import es.us.lsi.hermes.google.directions.Duration;

public class Element {

    private Distance distance;
    private Duration duration;

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
}
