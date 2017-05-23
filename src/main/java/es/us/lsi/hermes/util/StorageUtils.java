package es.us.lsi.hermes.util;

import es.us.lsi.hermes.simulator.PresetSimulation;
import es.us.lsi.hermes.simulator.SimulatorController;
import joptsimple.internal.Strings;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.logging.Level;
import java.util.logging.Logger;

public class StorageUtils {

    private static final Logger LOG = Logger.getLogger(Util.class.getName());

    // TODO Review merge
    private static Path createFolder(boolean temporal) {
        try {
            String folderStr;
            Path folderPath;

            if (temporal) {
                folderPath = Files.createTempDirectory("Hermes_Simulator");
                folderStr = folderPath.toAbsolutePath().toString() + File.separator;
            } else {
                File directory = new File("Hermes_Simulator_CSV");
                directory.mkdir();
                folderPath = directory.toPath();
                folderStr = folderPath.toString() + File.separator;
            }
            LOG.log(Level.INFO, "createFolder() - Directorio temporal para almacenar los CSV: {0}", folderStr);
            return folderPath;
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, "createFolder() - No se ha podido generar el directorio", ex);
            return null;
        }
    }

    @SuppressWarnings("all")
    static void clearFolderContent(Path folderPath) {
        try {
            for (File pathFile : folderPath.toFile().listFiles()) {
                pathFile.delete();
            }
        } catch (NullPointerException | SecurityException ex) {
            LOG.log(Level.SEVERE, "Error deleting the files located at path: " + folderPath, ex);
        }
    }

    public static Path createTempFolder() {
        try {
            // Creamos un directorio temporal para contener los archivos generados.
            Path tempFolder = Files.createTempDirectory("Hermes_Simulator");
            String tempFolderPath = tempFolder.toAbsolutePath().toString() + File.separator;

            LOG.log(Level.INFO, "createTempFolder() - Directorio temporal para almacenar los CSV: {0}", tempFolderPath);
            return tempFolder;
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, "createTempFolder() - No se ha podido generar el archivo con los datos de todos los eventos y los estados del simulador", ex);
            return null;
        }
    }

    static Path createCsvFolders(String subFolder) {
        String formattedPath = PresetSimulation.getPathForCsvStorage();

        try {
            // Creamos un directorio para contener los CSV generados.
            File directory = new File(formattedPath + File.separator + subFolder);
            String tempFolderPath = directory.toPath().toString() + File.separator;
            directory.mkdir();

            LOG.log(Level.INFO, "createTempFolder() - Directorio permanente para almacenar los CSV: {0}", tempFolderPath);
            return directory.toPath();
        } catch (SecurityException ex) {
            LOG.log(Level.SEVERE, "createTempFolder() - No se ha podido generar el archivo con los datos de todos los eventos y los estados del simulador", ex);
            return null;
        }
    }

    static File generateCsvFile(String fileNameHeader, String fileNameWithExtension, String subfolder) {
        Path folder;
        switch (subfolder) {
            case "drivers":
                folder = CSVUtils.PERMANENT_FOLDER_DRIVERS;
                break;
            case "paths":
                folder = CSVUtils.PERMANENT_FOLDER_PATHS;
                break;
            default:
                folder = SimulatorController.getTempFolder();
        }
        String eventsFileNameCSV = fileNameHeader + fileNameWithExtension;

        LOG.log(Level.INFO, "generateCsvFile() - Generando archivo CSV: {0}", eventsFileNameCSV);
        return new File(folder.toUri().getPath(), eventsFileNameCSV);
    }

    public static boolean canWrite(String path) {
        try {
            File dir = new File(path);
            dir.mkdir();
            return !Strings.isNullOrEmpty(path) && dir.canWrite();
        } catch (Exception ex) {
            return false;
        }
    }
}
