package es.us.lsi.hermes.util;

import es.us.lsi.hermes.csv.ICSVBean;
import es.us.lsi.hermes.simulator.SimulatorController;
import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.exception.ZipException;
import net.lingala.zip4j.model.ZipParameters;
import net.lingala.zip4j.util.Zip4jConstants;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvBeanWriter;
import org.supercsv.io.ICsvBeanWriter;
import org.supercsv.prefs.CsvPreference;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class CSVUtils {

    public static void createRouteDataFile(String fileNameHeader, List<ICSVBean> locationList, Logger LOG) {
        File routeFile = generateFile(fileNameHeader, "_route.csv", LOG);
        exportToCSV(CsvPreference.EXCEL_NORTH_EUROPE_PREFERENCE, false, routeFile, locationList, LOG);
    }

    public static void createStatusDataFile(String formattedCurrentTime, List<ICSVBean> itemList, Logger LOG){
        File statusFile = generateFile(formattedCurrentTime, "_status.csv", LOG);
        exportToCSV(CsvPreference.EXCEL_NORTH_EUROPE_PREFERENCE, false, statusFile, itemList, LOG);
    }

    public static void createEventsDataFile(String formattedCurrentTime, List<ICSVBean> itemList, Logger LOG){
        File eventsFile = generateFile(formattedCurrentTime,"_events.csv", LOG);
        exportToCSV(CsvPreference.EXCEL_NORTH_EUROPE_PREFERENCE, false, eventsFile, itemList, LOG);
    }

    private static void exportToCSV(CsvPreference csvPreference, boolean ignoreHeaders, File file, List<ICSVBean> itemList, Logger LOG) {

        // Integrity check
        if (itemList == null || itemList.isEmpty()) {
            LOG.log(Level.INFO, "Failure to export the provided list to the CSV: " + file.getName());
            return;
        }

        ICsvBeanWriter beanWriter = null;

        try {
            beanWriter = new CsvBeanWriter(new FileWriter(file), csvPreference);

            ICSVBean bean = itemList.get(0);
            // Seleccionamos los atributos que vamos a exportar.
            final String[] fields = bean.getFields();

            // Aplicamos las características de los campos.
            final CellProcessor[] processors = bean.getProcessors();

            // If requested: Set the headers.
            if (!ignoreHeaders)
                beanWriter.writeHeader(bean.getHeaders() != null ? bean.getHeaders() : fields);

            // Procesamos los elementos.
            for (final ICSVBean element : itemList)
                beanWriter.write(element, fields, processors);

        } catch (IOException ex) {
            LOG.log(Level.SEVERE, "exportToCSV() - Error al exportar a CSV: " + file.getName(), ex);

        } finally {
            // Cerramos.
            try {
                if (beanWriter != null)
                    beanWriter.close();
            } catch (IOException ex) {
                LOG.log(Level.SEVERE, "exportToCSV() - Error al cerrar el 'writer'", ex);
            }
        }
    }

    private static File generateFile(String fileNameHeader, String fileNameWithExtension, Logger LOG){;
        Path tempFolder = SimulatorController.getTempFolder();
        String eventsFileNameCSV = fileNameHeader + fileNameWithExtension;

        LOG.log(Level.INFO, "generateZippedCSV() - Generando archivo CSV: {0}", eventsFileNameCSV);
        return new File(tempFolder.toUri().getPath(), eventsFileNameCSV);
    }

    public static List<String> generateZippedCSV(List<ICSVBean> csvEventList, List<ICSVBean> csvStatusList, Logger LOG) {
        List<String> zipFilesPathsList = new ArrayList<>();

        try {
            String formattedCurrentTime = Constants.dfFile.format(System.currentTimeMillis());

            if (csvEventList != null && !csvEventList.isEmpty()) {
                // Creamos un archivo temporal para el CSV con los datos de los eventos.
                CSVUtils.createEventsDataFile(formattedCurrentTime, csvEventList, LOG);
            }

            // Creamos un archivo temporal para el CSV con los estados de la simulación.
            CSVUtils.createStatusDataFile(formattedCurrentTime, csvStatusList, LOG);

            // Creamos el archivo ZIP.
            ZipFile zipFile = new ZipFile(formattedCurrentTime + ".zip");

            // Inicializamos los parámetros de compresión del ZIP.
            ZipParameters parameters = new ZipParameters();
            parameters.setCompressionMethod(Zip4jConstants.COMP_DEFLATE);
            parameters.setCompressionLevel(Zip4jConstants.DEFLATE_LEVEL_ULTRA);

            // Creamos un archivo múltiple si supera los 25MB.
            File[] temporalFolderFiles = SimulatorController.getTempFolder().toFile().listFiles();
            if(temporalFolderFiles != null)
                zipFile.createZipFile(new ArrayList<>(Arrays.asList(temporalFolderFiles)), parameters, true, Constants.ZIP_FILE_SIZE);

            zipFilesPathsList = zipFile.getSplitZipFiles();
        } catch (ZipException ex) {
            LOG.log(Level.SEVERE, "generateZippedCSV() - Error al crear el ZIP con los datos de todos los eventos y los estados del simulador", ex);
        }

        return zipFilesPathsList;
    }
}