package es.us.lsi.hermes.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;

public class SaveImageFromURL {

    public static void savePngImage(String imageUrl, String fileName, File destinationFile) throws IOException {
        URL url = new URL(imageUrl);
        OutputStream os = null;

        try (InputStream is = url.openStream()) {
            if (fileName == null || fileName.length() == 0) {
                fileName = String.valueOf(System.currentTimeMillis());
            }
            os = new FileOutputStream(destinationFile + File.separator + fileName + ".png");
            byte[] b = new byte[2048];
            int length;
            while ((length = is.read(b)) != -1) {
                os.write(b, 0, length);
            }
        } finally {
            if (os != null) {
                os.close();
            }
        }
    }

    private SaveImageFromURL() {
    }

}
