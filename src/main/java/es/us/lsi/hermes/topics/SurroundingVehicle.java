package es.us.lsi.hermes.topics;

import java.io.Serializable;
import java.util.Set;

public class SurroundingVehicle implements Serializable {

    private String id;
    private Set<String> surroundingVehicles;

    public String getId() {
        return id;
    }

    public Set<String> getSurroundingVehicles() {
        return surroundingVehicles;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setSurroundingVehicles(Set<String> surroundingVehicles) {
        this.surroundingVehicles = surroundingVehicles;
    }
}
