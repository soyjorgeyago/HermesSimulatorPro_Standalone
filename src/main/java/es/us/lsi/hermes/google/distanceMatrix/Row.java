package es.us.lsi.hermes.google.distanceMatrix;

import java.util.ArrayList;
import java.util.List;

public class Row {

    private List<Element> elements = new ArrayList<>();

    /**
     *
     * @return The elements
     */
    public List<Element> getElements() {
        return elements;
    }

    /**
     *
     * @param elements The elements
     */
    public void setElements(List<Element> elements) {
        this.elements = elements;
    }

}
