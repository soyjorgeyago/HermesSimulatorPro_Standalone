package es.us.lsi.hermes.util;

import java.text.MessageFormat;
import java.util.ResourceBundle;

/**
 * Excepción controlada para Hermes. Su uso será para poder mostrar mensajes
 * informativos internacionalizados al usuario, de posibles excepciones de
 * cualquier tipo que pudieran ocurrir en el sistema.
 */
public class HermesException extends Exception {

    private static ResourceBundle bundle = ResourceBundle.getBundle("/Bundle");
    private final int code;

    public HermesException() {
        this.code = 0;
    }

    public HermesException(String key) {
        super(bundle.getString(key));
        this.code = 0;
    }

    public HermesException(String key, Object... params) {
        super(MessageFormat.format(bundle.getString(key), params));
        this.code = 0;
    }

    public HermesException(int code) {
        super();
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}
