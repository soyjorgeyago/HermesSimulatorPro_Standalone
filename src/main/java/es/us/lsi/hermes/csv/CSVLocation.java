package es.us.lsi.hermes.csv;

import org.supercsv.cellprocessor.constraint.NotNull;
import org.supercsv.cellprocessor.ift.CellProcessor;

import java.io.Serializable;

public class CSVLocation implements Serializable, ICSVBean {

    private Double lat;
    private Double lng;

    protected CellProcessor[] cellProcessors;
    protected String[] fields;
    protected String[] headers;

    public CSVLocation(Double lat, Double lng) {
        this.lat = lat;
        this.lng = lng;
        init();
    }

    @Override
    public void init() {
        cellProcessors = new CellProcessor[]{new NotNull(), new NotNull()};

        headers = new String[]{"Latitude", "Longitude"};

        fields = new String[]{"lat", "lng"};
    }

    @Override
    public CellProcessor[] getProcessors() {
        return cellProcessors;
    }

    @Override
    public String[] getFields() {
        return fields;
    }

    @Override
    public String[] getHeaders() {
        return headers;
    }

    public Double getLat() {
        return lat;
    }

    public Double getLng() {
        return lng;
    }
}