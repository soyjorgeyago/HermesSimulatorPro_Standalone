package es.us.lsi.hermes.util;

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
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class CSVUtils {

    private static final Logger LOG = Logger.getLogger(SimulatorController.class.getName());

    static final Path PERMANENT_FOLDER_PATHS = StorageUtils.createCsvFolders("Paths"),
            PERMANENT_FOLDER_DRIVERS = StorageUtils.createCsvFolders("Drivers");

    public static void createDriversDataFile(String fileNameHeader, List<DriverParameters> driversList) {
        File driversFile = StorageUtils.generateCsvFile(fileNameHeader, "_driver.csv", "drivers");
        exportToCSV(CsvPreference.EXCEL_NORTH_EUROPE_PREFERENCE, false, driversFile, DriverParameters.getHeaders(), DriverParameters.getFields(), DriverParameters.getProcessors(), driversList);
    }

    static void createRouteDataFile(String fileNameHeader, List<LocationLogDetail> locationList) {
        File routesFile = StorageUtils.generateCsvFile(fileNameHeader, "_path.csv", "paths");
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

    public static List<LocationLog> loadPathsFromFolder() {
        if (PERMANENT_FOLDER_PATHS == null) {
            return null;
        }

        List<List<LocationLogDetail>> pathsLoaded = loadAllPathFiles(PERMANENT_FOLDER_PATHS,
                CsvPreference.EXCEL_NORTH_EUROPE_PREFERENCE, PresetSimulation.getPathsAmount(), LocationLogDetail.getProcessors());

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

    private static List<List<LocationLogDetail>> loadAllPathFiles(Path folder, CsvPreference csvPreference, int maxFiles, CellProcessor[] cellProcessors) {
        File[] temporalFolderFiles = folder.toFile().listFiles();
        List<List<LocationLogDetail>> loadedItems = new ArrayList<>();

        int csvCounter = 0;
        if (temporalFolderFiles != null) {
            for (File aux : temporalFolderFiles) {
                if (aux.getName().contains(".csv")) {
                    loadedItems.add(loadPathsFiles(aux, csvPreference, cellProcessors));
                    csvCounter++;
                }

                // Generate as many paths as requested
                if (csvCounter >= maxFiles) {
                    break;
                }
            }
        }

        return loadedItems;
    }
    
    private static List<List<DriverParameters>> loadXSimulatedDriversFromFolder(Path folder, CsvPreference csvPreference, int maxFiles, int maxByFile) {
        File[] temporalFolderFiles = folder.toFile().listFiles();
        List<List<DriverParameters>> loadedItems = new ArrayList<>();

        int csvCounter = 0;
        if (temporalFolderFiles != null) {
            for (File aux : temporalFolderFiles) {
                if (aux.getName().contains(".csv")) {
                    loadedItems.add(loadLimitedCsvFile(aux, csvPreference, DriverParameters.getProcessors(), maxByFile));
                    csvCounter++;
                }

                // Generate as many paths as requested
                if (csvCounter >= maxFiles) {
                    break;
                }
            }
        }

        return loadedItems;
    }

    private static List<LocationLogDetail> loadPathsFiles(File file, CsvPreference csvPreference, CellProcessor[] cellProcessors) {
        List<LocationLogDetail> result = new ArrayList<>();
        ICsvBeanReader beanReader = null;

        try {
            beanReader = new CsvBeanReader(new FileReader(file), csvPreference);

            // the header elements are used to map the values to the bean (names must match)
            final String[] header = beanReader.getHeader(true);

            // While there are new lines, save those into the list
            LocationLogDetail lineTemp;
            while ((lineTemp = beanReader.read(LocationLogDetail.class, header, cellProcessors)) != null) {
                result.add(lineTemp);
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

        return result;
    }
    
    private static List<DriverParameters> loadLimitedCsvFile(File file, CsvPreference csvPreference, CellProcessor[] cellProcessors, int maxByFile) {
        List<DriverParameters> result = new ArrayList<>();
        ICsvBeanReader beanReader = null;

        try {
            beanReader = new CsvBeanReader(new FileReader(file), csvPreference);

            // the header elements are used to map the values to the bean (names must match)
            final String[] header = beanReader.getHeader(true);

            // While there are new lines, save those into the list
            int i = 0;
            DriverParameters lineTemp = beanReader.read(DriverParameters.class, header, cellProcessors);
            while (lineTemp != null) {
                result.add(lineTemp);
                i++;
                if (i >= maxByFile) {
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
            } catch (IOException ex) {
                LOG.log(Level.WARNING, "Exception closing the beanReader", ex);
            }
        }

        return result;
    }

    //FIXME delete files from drivers/paths before creating new ones
    public static List<List<DriverParameters>> loadSimulatedDriversForPath() {
        if (PERMANENT_FOLDER_DRIVERS == null) {
            return null;
        }

        return loadXSimulatedDriversFromFolder(PERMANENT_FOLDER_DRIVERS, CsvPreference.EXCEL_NORTH_EUROPE_PREFERENCE, PresetSimulation.getPathsAmount(), PresetSimulation.getDriversByPath());
    }
}
