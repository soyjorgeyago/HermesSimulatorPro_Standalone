package es.us.lsi.hermes.csv;

import org.supercsv.cellprocessor.ift.CellProcessor;


public interface ICSVBean {
    
    void init();

    CellProcessor[] getProcessors();

    String[] getFields();

    String[] getHeaders();
}
