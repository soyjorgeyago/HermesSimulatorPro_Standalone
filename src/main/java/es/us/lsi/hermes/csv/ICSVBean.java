package es.us.lsi.hermes.csv;

import org.supercsv.cellprocessor.ift.CellProcessor;


public interface ICSVBean {
    
    public void init();

    public CellProcessor[] getProcessors();

    public String[] getFields();

    public String[] getHeaders();

}
