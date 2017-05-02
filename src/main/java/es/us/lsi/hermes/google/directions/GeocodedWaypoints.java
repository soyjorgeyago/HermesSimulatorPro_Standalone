package es.us.lsi.hermes.google.directions;

import java.util.ArrayList;
import java.util.List;

public class GeocodedWaypoints {

    private List<Route> routes = new ArrayList<>();

    /**
     *
     * @return The routes
     */
    public List<Route> getRoutes() {
        return routes;
    }

    /**
     *
     * @param routes The routes
     */
    public void setRoutes(List<Route> routes) {
        this.routes = routes;
    }
}
