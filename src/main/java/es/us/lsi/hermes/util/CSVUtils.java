package es.us.lsi.hermes.util;

import es.us.lsi.hermes.csv.ICSVBean;
import es.us.lsi.hermes.location.LocationLog;
import es.us.lsi.hermes.location.detail.LocationLogDetail;
import es.us.lsi.hermes.person.Person;
import es.us.lsi.hermes.simulator.PresetSimulation;
import es.us.lsi.hermes.simulator.SimulatedSmartDriver;
import es.us.lsi.hermes.simulator.SimulatorController;
import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.exception.ZipException;
import net.lingala.zip4j.model.ZipParameters;
import net.lingala.zip4j.util.Zip4jConstants;
import org.supercsv.cellprocessor.ParseDouble;
import org.supercsv.cellprocessor.ParseInt;
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

    static final Path PERMANENT_FOLDER_PATHS = StorageUtils.createCsvFolders("Paths"),
            PERMANENT_FOLDER_DRIVERS = StorageUtils.createCsvFolders("Drivers");

    public static void createDriversDataFile(String fileNameHeader, List<ICSVBean> driversList) {
        File driversFile = StorageUtils.generateCsvFile(fileNameHeader, "_driver.csv", "drivers");
        exportToCSV(CsvPreference.EXCEL_NORTH_EUROPE_PREFERENCE, false, driversFile, driversList);
    }

    static void createRouteDataFile(String fileNameHeader, List<ICSVBean> locationList) {
        File routesFile = StorageUtils.generateCsvFile(fileNameHeader, "_path.csv", "paths");
        exportToCSV(CsvPreference.EXCEL_NORTH_EUROPE_PREFERENCE, false, routesFile, locationList);
    }

    private static void createStatusDataFile(String formattedCurrentTime, List<ICSVBean> statusList) {
        File statusFile = StorageUtils.generateCsvFile(formattedCurrentTime, "_status.csv", null);
        exportToCSV(CsvPreference.EXCEL_NORTH_EUROPE_PREFERENCE, false, statusFile, statusList);
    }

    private static void createEventsDataFile(String formattedCurrentTime, List<ICSVBean> eventList) {
        File eventsFile = StorageUtils.generateCsvFile(formattedCurrentTime, "_events.csv", null);
        exportToCSV(CsvPreference.EXCEL_NORTH_EUROPE_PREFERENCE, false, eventsFile, eventList);
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

    public static ArrayList generateZippedCSV(List<ICSVBean> csvEventList, List<ICSVBean> csvStatusList) {
        ArrayList zipFilesPathsList = new ArrayList();

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

    public static List<LocationLog> extractSimulatedPaths() {
        if (PERMANENT_FOLDER_PATHS == null) {
            return null;
        }

        List<List<ICSVBean>> extractedRoutes = extractFromFolder(PERMANENT_FOLDER_PATHS,
                CsvPreference.EXCEL_NORTH_EUROPE_PREFERENCE, PresetSimulation.getPathsAmount(),  new CellProcessor[]{
                new ParseDouble(), new ParseDouble(), new ParseDouble(), new ParseInt(), new ParseInt()});



        if(extractedRoutes.size() == 0) {
            LOG.log(Level.SEVERE, "No CSV files found under the directory: {0}", PERMANENT_FOLDER_PATHS);
        } else if(extractedRoutes.size() < PresetSimulation.getPathsAmount()) {
            LOG.log(Level.SEVERE, "{0} CSV files found under the directory: {1}, {2} paths requested", new Object[]{extractedRoutes.size(), PERMANENT_FOLDER_PATHS, PresetSimulation.getPathsAmount()});
        }


        //TODO Remove show
        // USE the extracted data
        for (List<ICSVBean> aux : extractedRoutes) {
            System.out.println(aux);
        }

        // Set the extracted routes as the DataSet to use
        List<LocationLog> locationLogList = new ArrayList<>();

        for(List<ICSVBean> route : extractedRoutes){
            LocationLog routeLog = new LocationLog();

            routeLog.setLocationLogId(0);
            routeLog.setDistance(0);
            routeLog.setDuration(0);

            routeLog.setLocationLogDetailList(route);

            Person person = Person.createSimimulatedPerson();
            routeLog.setPerson(person);
            routeLog.setFilename(person.getFullName());

            locationLogList.add(routeLog);
        }

        if(locationLogList.size() < PresetSimulation.getPathsAmount()) {
            LOG.log(Level.SEVERE, "CSV Paths available: {0}, Paths requested: {1}", new Object[]{locationLogList.size(), PresetSimulation.getPathsAmount()});
        }

        return locationLogList;
    }

    private static List<List<ICSVBean>> extractFromFolder(Path folder, CsvPreference csvPreference, int maxByFile, CellProcessor[] cellProcessors){
        File[] temporalFolderFiles = folder.toFile().listFiles();
        List<List<ICSVBean>> extractedItems = new ArrayList<>();

        int csvCounter = 0;
        if (temporalFolderFiles != null) {
            for (File aux : temporalFolderFiles) {
                if(aux.getName().contains(".csv")){
                    extractedItems.add(extractFromSingleFile(aux, csvPreference, cellProcessors));
                    csvCounter++;
                }

                // Generate as many paths as requested
                if(csvCounter == maxByFile)
                    break;
            }
        }

        return extractedItems;
    }

    private static List<ICSVBean> extractFromSingleFile(File file, CsvPreference csvPreference, CellProcessor[] cellProcessors) {
        List<ICSVBean> extractedRoute = new ArrayList<>();
        ICsvBeanReader beanReader = null;

        try {
            beanReader = new CsvBeanReader(new FileReader(file), csvPreference);

            // the header elements are used to map the values to the bean (names must match)
            final String[] header = beanReader.getHeader(true);

            // While there are new lines, save those into the list
            ICSVBean lineTemp;
            while ((lineTemp = beanReader.read(cellProcessors.length >= 5 ? LocationLogDetail.class : SimulatedSmartDriver.class, header, cellProcessors)) != null) {
                extractedRoute.add(lineTemp);
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

    //FIXME delete files from drivers/paths before creating new ones
    public static List<List<ICSVBean>> extractSimulatedDriverForPath() {
        if (PERMANENT_FOLDER_DRIVERS == null) {
            return null;
        }

        return extractFromFolder(PERMANENT_FOLDER_DRIVERS, CsvPreference.EXCEL_NORTH_EUROPE_PREFERENCE, PresetSimulation.getDriversByPath(),  new CellProcessor[]{new ParseDouble(), new ParseDouble()});
    }
}
