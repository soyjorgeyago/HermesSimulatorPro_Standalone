package es.us.lsi.hermes.google.directions;

import java.util.ArrayList;
import java.util.List;

public class Route {

    private List<Leg> legs = new ArrayList<>();
    private OverviewPolyline overviewPolyline;
    private String summary;

    /**
     *
     * @return The legs
     */
    public List<Leg> getLegs() {
        return legs;
    }

    /**
     *
     * @param legs The legs
     */
    public void setLegs(List<Leg> legs) {
        this.legs = legs;
    }

    /**
     *
     * @return The overviewPolyline
     */
    public OverviewPolyline getOverviewPolyline() {
        return overviewPolyline;
    }

    /**
     *
     * @param overviewPolyline The overview_polyline
     */
    public void setOverviewPolyline(OverviewPolyline overviewPolyline) {
        this.overviewPolyline = overviewPolyline;
    }

    /**
     *
     * @return The summary
     */
    public String getSummary() {
        return summary;
    }

    /**
     *
     * @param summary The summary
     */
    public void setSummary(String summary) {
        this.summary = summary;
    }
}
