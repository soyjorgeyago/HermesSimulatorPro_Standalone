package es.us.lsi.hermes.util;

import com.squareup.okhttp.HttpUrl;
import es.us.lsi.hermes.location.LocationLog;
import es.us.lsi.hermes.location.detail.LocationLogDetail;
import es.us.lsi.hermes.simulator.PresetSimulation;
import es.us.lsi.hermes.simulator.SimulatorController;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvBeanReader;
import org.supercsv.io.CsvBeanWriter;
import org.supercsv.io.ICsvBeanReader;
import org.supercsv.io.ICsvBeanWriter;
import org.supercsv.prefs.CsvPreference;

import java.io.*;
import java.net.URL;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class CSVUtils {

    private static final Logger LOG = Logger.getLogger(SimulatorController.class.getName());

    static final Path PERMANENT_FOLDER_PATHS = StorageUtils.createCsvFolders(Constants.PATHS_FOLDER),
            PERMANENT_FOLDER_DRIVERS = StorageUtils.createCsvFolders(Constants.DRIVERS_FOLDER);

    public static void createDriversDataFile(String fileNameHeader, List<DriverParameters> driversList) {
        File driversFile = StorageUtils.generateCsvFile(fileNameHeader, Constants.DRIVERS_EXTENSION, Constants.DRIVERS_FOLDER);
        exportToCSV(CsvPreference.EXCEL_NORTH_EUROPE_PREFERENCE, false, driversFile, DriverParameters.getHeaders(), DriverParameters.getFields(), DriverParameters.getProcessors(), driversList);
    }

    static void createRouteDataFile(String fileNameHeader, List<LocationLogDetail> locationList) {
        File routesFile = StorageUtils.generateCsvFile(fileNameHeader, Constants.PATHS_EXTENSION, Constants.PATHS_FOLDER);
        exportToCSV(CsvPreference.EXCEL_NORTH_EUROPE_PREFERENCE, false, routesFile,  LocationLogDetail.getHeaders(), LocationLogDetail.getFields(), LocationLogDetail.getProcessors(), locationList);
    }

    private static void exportToCSV(CsvPreference csvPreference, boolean ignoreHeaders, File file, String[] headers, String[] fields, CellProcessor[] cellProcessors, List<?> itemList) {

        // Integrity check
        if (itemList == null || itemList.isEmpty()) {
            LOG.log(Level.INFO, "Failure to export the provided list to the CSV: {0}", file.getName());
            return;
        }

        ICsvBeanWriter beanWriter = null;

        try {
            beanWriter = new CsvBeanWriter(new FileWriter(file), csvPreference);

            // If requested: Set the headers.
            if (!ignoreHeaders) {
                beanWriter.writeHeader(headers != null ? headers : fields);
            }

            // Procesamos los elementos.
            for (final Object element : itemList) {
                beanWriter.write(element, fields, cellProcessors);
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

    public static List<LocationLog> loadAllPaths() {
        if (PERMANENT_FOLDER_PATHS == null) {
            return null;
        }

        List<List<LocationLogDetail>> pathsLoaded;
        if(SimulatorController.isLocalMode()) {
            pathsLoaded = loadAllPathsFromFolder(PERMANENT_FOLDER_PATHS, CsvPreference.EXCEL_NORTH_EUROPE_PREFERENCE);
        } else {
            pathsLoaded = loadAllPathsFromServer(CsvPreference.EXCEL_NORTH_EUROPE_PREFERENCE);
        }

        if (pathsLoaded.isEmpty()) {
            LOG.log(Level.SEVERE, "No CSV files found under the directory: {0}", PERMANENT_FOLDER_PATHS);
        } else if (pathsLoaded.size() < PresetSimulation.getPathsAmount()) {
            LOG.log(Level.SEVERE, "{0} CSV files found under the directory: {1}, {2} paths requested", new Object[]{pathsLoaded.size(), PERMANENT_FOLDER_PATHS, PresetSimulation.getPathsAmount()});
        }

        // Set the extracted routes as the DataSet to use
        List<LocationLog> locationLogList = new ArrayList<>();

        int i = 0;
        for (List<LocationLogDetail> route : pathsLoaded) {
            LocationLog routeLog = new LocationLog();

            routeLog.setLocationLogId(i);

            routeLog.setLocationLogDetailList(route);

            locationLogList.add(routeLog);
            i++;
        }

        if (locationLogList.size() < PresetSimulation.getPathsAmount()) {
            LOG.log(Level.SEVERE, "CSV Paths available: {0}, Paths requested: {1}", new Object[]{locationLogList.size(), PresetSimulation.getPathsAmount()});
        }

        return locationLogList;
    }

    //FIXME delete files from drivers/paths before creating new ones
    public static List<List<DriverParameters>> loadDrivers() {
        if (PERMANENT_FOLDER_DRIVERS == null) {
            return null;
        }
        if(SimulatorController.isLocalMode()) {
            return loadAllDriversFromFolder(PERMANENT_FOLDER_DRIVERS, CsvPreference.EXCEL_NORTH_EUROPE_PREFERENCE);
        } else {
            return loadAllDriversFromServer(CsvPreference.EXCEL_NORTH_EUROPE_PREFERENCE);
        }
    }

    private static List<List<LocationLogDetail>> loadAllPathsFromFolder(Path folder, CsvPreference csvPreference) {
        File[] temporalFolderFiles = folder.toFile().listFiles();
        List<List<LocationLogDetail>> loadedItems = new ArrayList<>();

        int csvCounter = 0;
        if (temporalFolderFiles != null) {
            for (File aux : temporalFolderFiles) {
                if (aux.getName().contains(".csv")) {
                    loadedItems.add(loadPathFromLocalFile(aux, csvPreference));
                    csvCounter++;
                }

                // Generate as many paths as requested
                if (csvCounter >= PresetSimulation.getPathsAmount()) {
                    break;
                }
            }
        }

        return loadedItems;
    }

    private static List<List<DriverParameters>> loadAllDriversFromFolder(Path folder, CsvPreference csvPreference) {
        File[] temporalFolderFiles = folder.toFile().listFiles();
        List<List<DriverParameters>> loadedItems = new ArrayList<>();

        int csvCounter = 0;
        if (temporalFolderFiles != null) {
            for (File aux : temporalFolderFiles) {
                if (aux.getName().contains(".csv")) {
                    loadedItems.add(loadDriverParamsFromLocalFile(aux, csvPreference));
                    csvCounter++;
                }

                // Generate as many paths as requested
                if (csvCounter >= PresetSimulation.getPathsAmount()) {
                    break;
                }
            }
        }

        return loadedItems;
    }

    private static List<List<LocationLogDetail>> loadAllPathsFromServer(CsvPreference csvPreference) {
        List<List<LocationLogDetail>> loadedItems = new ArrayList<>();

        for (int i = 1; i <= PresetSimulation.getPathsAmount(); i++) {
            loadedItems.add(loadPathFromServerFile(i, csvPreference));
        }

        return loadedItems;
    }

    private static List<List<DriverParameters>> loadAllDriversFromServer(CsvPreference csvPreference) {
        List<List<DriverParameters>> loadedItems = new ArrayList<>();

        for (int i = 1; i <= PresetSimulation.getPathsAmount(); i++) {
            loadedItems.add(loadDriverParamsFromServerFile(i, csvPreference));
        }

        return loadedItems;
    }

    private static URL buildDriverParamsUrl(int fileNumber){
        return buildResourceUrl(fileNumber, false);
    }

    private static URL buildPathUrl(int fileNumber){
        return buildResourceUrl(fileNumber, true);
    }

    private static URL buildResourceUrl(int fileNumber, boolean isPath){
        try {
            return new HttpUrl.Builder()
                    .scheme("http")
                    .host(Constants.HERMES_SERVER)
                    .addPathSegment(Constants.SERVER_HERMES_FOLDER)
                    .addPathSegment(Constants.CSV_FOLDER)
                    .addPathSegment(isPath ? Constants.PATHS_FOLDER : Constants.DRIVERS_FOLDER)
                    .addPathSegment(fileNumber + (isPath ? Constants.PATHS_EXTENSION : Constants.DRIVERS_EXTENSION))
                    .build().url();
        }catch (Exception ex){
            LOG.log(Level.SEVERE, "The URL for the path " + fileNumber + " could not be built", ex);
            return null;
        }
    }

    private static List<LocationLogDetail> loadPathFromLocalFile(File file, CsvPreference csvPreference) {
        return loadPathFromFile(file, -1, csvPreference);
    }

    private static List<LocationLogDetail> loadPathFromServerFile(int fileNumber, CsvPreference csvPreference) {
        return loadPathFromFile(null, fileNumber, csvPreference);
    }

    private static List<LocationLogDetail> loadPathFromFile(File file, int fileNumber, CsvPreference csvPreference) {
        List<LocationLogDetail> result = new ArrayList<>();
        ICsvBeanReader beanReader = null;
        Reader mainReader = null;
        InputStreamReader streamReader = null;
        InputStream inputStream = null;

        try {
            if(SimulatorController.isLocalMode()){
                mainReader = new FileReader(file);
                beanReader = new CsvBeanReader(mainReader, csvPreference);
            }else{
                URL url = buildPathUrl(fileNumber);
                if(url == null) return result;
                inputStream = url.openStream();
                streamReader = new InputStreamReader(inputStream);
                mainReader = new BufferedReader(streamReader);
                beanReader = new CsvBeanReader(mainReader, csvPreference);
            }

            // the header elements are used to map the values to the bean (names must match)
            final String[] header = beanReader.getHeader(true);

            // While there are new lines, save those into the list
            LocationLogDetail lineTemp;
            while ((lineTemp = beanReader.read(LocationLogDetail.class, header, LocationLogDetail.getProcessors())) != null) {
                result.add(lineTemp);
            }

        } catch (IOException | NullPointerException ex) {
            LOG.log(Level.SEVERE, "Exception parsing the stored routes", ex);
        } finally {
            try {
                if (beanReader != null) {
                    beanReader.close();
                }
                if (mainReader != null) {
                    mainReader.close();
                }
                if (streamReader != null) {
                    streamReader.close();
                }
                if(inputStream != null){
                    inputStream.close();
                }
            } catch (IOException ex) {
                LOG.log(Level.WARNING, "Exception closing the beanReader", ex);
            }
        }

        return result;
    }

    private static List<DriverParameters> loadDriverParamsFromLocalFile(File file, CsvPreference csvPreference) {
        return loadDriverParamsFromFile(file, -1, csvPreference);
    }

    private static List<DriverParameters> loadDriverParamsFromServerFile(int fileNumber, CsvPreference csvPreference) {
        return loadDriverParamsFromFile(null, fileNumber, csvPreference);
    }

    private static List<DriverParameters> loadDriverParamsFromFile(File file, int fileNumber, CsvPreference csvPreference) {
        List<DriverParameters> result = new ArrayList<>();
        ICsvBeanReader beanReader = null;
        Reader mainReader = null;
        InputStreamReader streamReader = null;
        InputStream inputStream = null;

        try {
            if(SimulatorController.isLocalMode()){
                mainReader = new FileReader(file);
                beanReader = new CsvBeanReader(mainReader, csvPreference);
            }else{
                URL url = buildDriverParamsUrl(fileNumber);
                if(url == null) return result;
                inputStream = url.openStream();
                streamReader = new InputStreamReader(inputStream);
                mainReader = new BufferedReader(streamReader);
                beanReader = new CsvBeanReader(mainReader, csvPreference);
            }

            // the header elements are used to map the values to the bean (names must match)
            final String[] header = beanReader.getHeader(true);

            // While there are new lines, save those into the list
            int i = 0;
            DriverParameters lineTemp;
            while ((lineTemp = beanReader.read(DriverParameters.class, header, DriverParameters.getProcessors())) != null) {
                result.add(lineTemp);
                i++;
                if (i >= PresetSimulation.getDriversByPath()) {
                    break;
                }
            }

        } catch (IOException ex) {
            LOG.log(Level.SEVERE, "Exception parsing the stored routes", ex);
        } finally {
            try {
                if (beanReader != null) {
                    beanReader.close();
                }
                if (mainReader != null) {
                    mainReader.close();
                }
                if (streamReader != null) {
                    streamReader.close();
                }
                if(inputStream != null){
                    inputStream.close();
                }
            } catch (IOException ex) {
                LOG.log(Level.WARNING, "Exception closing the beanReader", ex);
            }
        }

        return result;
    }
}
