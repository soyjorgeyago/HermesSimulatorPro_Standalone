package es.us.lsi.hermes.csv;

import org.supercsv.cellprocessor.ift.CellProcessor;


public interface ICSVBean {
    
    void initCSV();

    CellProcessor[] getProcessors();

    String[] getFields();

    String[] getHeaders();
}
