package es.us.lsi.hermes.openStreetMap;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

public class PositionSimulatedSpeed {

    @SerializedName("speed")
    @Expose
    private Double speed;
    @SerializedName("position")
    @Expose
    private Position position;

    public Double getSpeed() {
        return speed;
    }

    public void setSpeed(Double speed) {
        this.speed = speed;
    }

    public Position getPosition() {
        return position;
    }

    public void setPosition(Position position) {
        this.position = position;
    }

}
