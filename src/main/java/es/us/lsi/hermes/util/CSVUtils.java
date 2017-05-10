package es.us.lsi.hermes.util;

import es.us.lsi.hermes.csv.CSVLocation;
import es.us.lsi.hermes.csv.ICSVBean;
import es.us.lsi.hermes.simulator.SimulatorController;
import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.exception.ZipException;
import net.lingala.zip4j.model.ZipParameters;
import net.lingala.zip4j.util.Zip4jConstants;
import org.supercsv.cellprocessor.constraint.NotNull;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvBeanReader;
import org.supercsv.io.CsvBeanWriter;
import org.supercsv.io.ICsvBeanReader;
import org.supercsv.io.ICsvBeanWriter;
import org.supercsv.prefs.CsvPreference;

import java.io.*;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class CSVUtils {

    private static final Logger LOG = Logger.getLogger(SimulatorController.class.getName());

    private static final Path PERMANENT_FOLDER = Util.getOrCreateCsvFolder();

    public static void createRouteDataFile(String fileNameHeader, List<ICSVBean> locationList) {
        File routeFile = generateFile(fileNameHeader, "_route.csv", true);
        exportToCSV(CsvPreference.EXCEL_NORTH_EUROPE_PREFERENCE, false, routeFile, locationList);
    }

    private static void createStatusDataFile(String formattedCurrentTime, List<ICSVBean> itemList) {
        File statusFile = generateFile(formattedCurrentTime, "_status.csv", false);
        exportToCSV(CsvPreference.EXCEL_NORTH_EUROPE_PREFERENCE, false, statusFile, itemList);
    }

    private static void createEventsDataFile(String formattedCurrentTime, List<ICSVBean> itemList) {
        File eventsFile = generateFile(formattedCurrentTime, "_events.csv", false);
        exportToCSV(CsvPreference.EXCEL_NORTH_EUROPE_PREFERENCE, false, eventsFile, itemList);
    }

    private static void exportToCSV(CsvPreference csvPreference, boolean ignoreHeaders, File file, List<ICSVBean> itemList) {

        // Integrity check
        if (itemList == null || itemList.isEmpty()) {
            LOG.log(Level.INFO, "Failure to export the provided list to the CSV: {0}", file.getName());
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
            if (!ignoreHeaders) {
                beanWriter.writeHeader(bean.getHeaders() != null ? bean.getHeaders() : fields);
            }

            // Procesamos los elementos.
            for (final ICSVBean element : itemList) {
                beanWriter.write(element, fields, processors);
            }

        } catch (IOException ex) {
            LOG.log(Level.SEVERE, "exportToCSV() - Error al exportar a CSV: " + file.getName(), ex);

        } finally {
            // Cerramos.
            try {
                if (beanWriter != null) {
                    beanWriter.close();
                }
            } catch (IOException ex) {
                LOG.log(Level.SEVERE, "exportToCSV() - Error al cerrar el 'writer'", ex);
            }
        }
    }

    private static File generateFile(String fileNameHeader, String fileNameWithExtension, boolean permanent) {
        Path tempFolder = permanent ? PERMANENT_FOLDER : SimulatorController.getTempFolder();
        String eventsFileNameCSV = fileNameHeader + fileNameWithExtension;

        LOG.log(Level.INFO, "generateZippedCSV() - Generando archivo CSV: {0}", eventsFileNameCSV);
        return new File(tempFolder.toUri().getPath(), eventsFileNameCSV);
    }

    public static List<String> generateZippedCSV(List<ICSVBean> csvEventList, List<ICSVBean> csvStatusList) {
        List<String> zipFilesPathsList = new ArrayList<>();

        try {
            String formattedCurrentTime = Constants.dfFile.format(System.currentTimeMillis());

            if (csvEventList != null && !csvEventList.isEmpty()) {
                // Creamos un archivo temporal para el CSV con los datos de los eventos.
                createEventsDataFile(formattedCurrentTime, csvEventList);
            }

            // Creamos un archivo temporal para el CSV con los estados de la simulación.
            createStatusDataFile(formattedCurrentTime, csvStatusList);

            // Creamos el archivo ZIP.
            ZipFile zipFile = new ZipFile(formattedCurrentTime + ".zip");

            // Inicializamos los parámetros de compresión del ZIP.
            ZipParameters parameters = new ZipParameters();
            parameters.setCompressionMethod(Zip4jConstants.COMP_DEFLATE);
            parameters.setCompressionLevel(Zip4jConstants.DEFLATE_LEVEL_ULTRA);

            // Creamos un archivo múltiple si supera los 25MB.
            File[] temporalFolderFiles = SimulatorController.getTempFolder().toFile().listFiles();
            if (temporalFolderFiles != null) {
                zipFile.createZipFile(new ArrayList<>(Arrays.asList(temporalFolderFiles)), parameters, true, Constants.ZIP_FILE_SIZE);
            }

            zipFilesPathsList = zipFile.getSplitZipFiles();
        } catch (ZipException ex) {
            LOG.log(Level.SEVERE, "generateZippedCSV() - Error al crear el ZIP con los datos de todos los eventos y los estados del simulador", ex);
        }

        return zipFilesPathsList;
    }

    public static void extractSimulatedPaths() {
        if (PERMANENT_FOLDER == null) {
            return;
        }

        File[] temporalFolderFiles = PERMANENT_FOLDER.toFile().listFiles();

        List<List<ICSVBean>> extractedRoutes = new ArrayList<>();

        if (temporalFolderFiles != null) {
            for (File aux : temporalFolderFiles) {
                System.out.println(aux);
                extractedRoutes.add(extractSinglePath(aux, CsvPreference.EXCEL_NORTH_EUROPE_PREFERENCE));
            }
        }

        // USE the extracted data
        for (List<ICSVBean> aux : extractedRoutes) {
            System.out.println(aux);
        }
    }

    private static List<ICSVBean> extractSinglePath(File file, CsvPreference csvPreference) {
        List<ICSVBean> extractedRoute = new ArrayList<>();
        ICsvBeanReader beanReader = null;

        try {
            beanReader = new CsvBeanReader(new FileReader(file), csvPreference);

            // the header elements are used to map the values to the bean (names must match)
            final String[] header = beanReader.getHeader(true);
            final CellProcessor[] processors = new CellProcessor[]{new NotNull(), new NotNull()};

            // While there are new locations, save those into the route
            CSVLocation locationTemp;
            while ((locationTemp = beanReader.read(CSVLocation.class, header, processors)) != null) {
                extractedRoute.add(locationTemp);
            }

        } catch (IOException ex) {
            LOG.log(Level.SEVERE, "Exception parsing the stored routes", ex);
        } finally {
            try {
                if (beanReader != null) {
                    beanReader.close();
                }
            } catch (IOException ex) {
                LOG.log(Level.WARNING, "Exception closing the beanReader", ex);
            }
        }

        return extractedRoute;
    }
}
