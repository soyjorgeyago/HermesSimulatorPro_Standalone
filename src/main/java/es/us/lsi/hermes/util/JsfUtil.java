package es.us.lsi.hermes.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;
import javax.faces.application.FacesMessage;
import javax.faces.component.UIComponent;
import javax.faces.context.ExternalContext;
import javax.faces.context.FacesContext;
import javax.faces.convert.Converter;
import javax.faces.model.SelectItem;

public class JsfUtil {

    private JsfUtil() {
        // Para prevenir la instanciación de la clase.
    }

    public static SelectItem[] getSelectItems(List<?> entities, boolean selectOne) {
        int size = selectOne ? entities.size() + 1 : entities.size();
        SelectItem[] items = new SelectItem[size];
        int i = 0;
        if (selectOne) {
            items[0] = new SelectItem("", "---");
            i++;
        }
        for (Object x : entities) {
            items[i++] = new SelectItem(x, x.toString());
        }
        return items;
    }

    public static boolean isValidationFailed() {
        return FacesContext.getCurrentInstance().isValidationFailed();
    }

    public static void addErrorMessage(Exception ex, String defaultMsg) {
        String msg = ex.getLocalizedMessage();
        if (msg != null && msg.length() > 0) {
            addErrorMessage(msg);
        } else {
            addErrorMessage(defaultMsg);
        }
    }

    public static void addErrorMessages(List<String> messages) {
        for (String message : messages) {
            addErrorMessage(message);
        }
    }

    public static void addErrorMessage(String msg) {
        FacesMessage facesMsg = new FacesMessage(FacesMessage.SEVERITY_ERROR, msg, "");
        FacesContext.getCurrentInstance().addMessage("messages", facesMsg);
    }

    public static void addErrorMessageTag(String tag, String msg) {
        FacesMessage facesMsg = new FacesMessage(FacesMessage.SEVERITY_ERROR, msg, "");
        FacesContext.getCurrentInstance().addMessage(tag, facesMsg);
    }

    public static void addErrorMessage(String msg, String dtl) {
        FacesMessage facesMsg = new FacesMessage(FacesMessage.SEVERITY_ERROR, msg, dtl);
        FacesContext.getCurrentInstance().addMessage("messages", facesMsg);
    }

    public static void addErrorMessageTag(String tag, String msg, String dtl) {
        FacesMessage facesMsg = new FacesMessage(FacesMessage.SEVERITY_ERROR, msg, dtl);
        FacesContext.getCurrentInstance().addMessage(tag, facesMsg);
    }
    
    public static void addWarnMessage(String msg) {
        FacesMessage facesMsg = new FacesMessage(FacesMessage.SEVERITY_WARN, msg, "");
        FacesContext.getCurrentInstance().addMessage("messages", facesMsg);
    }

    public static void addInfoMessage(String msg) {
        FacesMessage facesMsg = new FacesMessage(FacesMessage.SEVERITY_INFO, msg, "");
        FacesContext.getCurrentInstance().addMessage("messages", facesMsg);
    }

    public static void addInfoMessageTag(String tag, String msg) {
        FacesMessage facesMsg = new FacesMessage(FacesMessage.SEVERITY_INFO, msg, "");
        FacesContext.getCurrentInstance().addMessage(tag, facesMsg);
    }

    public static void addInfoMessage(String msg, String dtl) {
        FacesMessage facesMsg = new FacesMessage(FacesMessage.SEVERITY_INFO, msg, dtl);
        FacesContext.getCurrentInstance().addMessage("messages", facesMsg);
    }

    public static void addInfoMessageTag(String tag, String msg, String dtl) {
        FacesMessage facesMsg = new FacesMessage(FacesMessage.SEVERITY_INFO, msg, dtl);
        FacesContext.getCurrentInstance().addMessage(tag, facesMsg);
    }

    public static String getRequestParameter(String key) {
        return FacesContext.getCurrentInstance().getExternalContext().getRequestParameterMap().get(key);
    }

    public static Object getObjectFromRequestParameter(String requestParameterName, Converter converter, UIComponent component) {
        String theId = JsfUtil.getRequestParameter(requestParameterName);
        return converter.getAsObject(FacesContext.getCurrentInstance(), component, theId);
    }

    /**
     * Método para mostrar un pequeño mensaje de ayuda, siempre que exista en el
     * XHTML la etiqueta '<p:message>' con el identificador 'helpMessage'. Se
     * creará una 'cookie' con vigencia de 10 años para que no se muestre el
     * mensaje más de una vez, a menos que limpie las 'cookies' ;)
     *
     * @param nameOfCookie Nombre de la 'cookie'
     * @param message Mensaje que se mostrará en la zona de mensajes.
     */
    public static void showHelpMessage(String nameOfCookie, String message) {
        ExternalContext externalContext = FacesContext.getCurrentInstance().getExternalContext();
        Map<String, Object> requestCookieMap = externalContext.getRequestCookieMap();

        // El mensaje de ayuda se mostrará si no existe la cookie.
        if (!requestCookieMap.containsKey(nameOfCookie)) {
            Map<String, Object> properties = new HashMap<>();
            properties.put("maxAge", 315360000); // 10 años ;)
            externalContext.addResponseCookie(nameOfCookie, Long.toString(System.currentTimeMillis()), properties);
            FacesContext.getCurrentInstance().addMessage("helpMessage", new FacesMessage(FacesMessage.SEVERITY_INFO, ResourceBundle.getBundle("/Bundle").getString("Information"), message));
        }
    }

    /**
     * Método para mostrar un pequeño mensaje de ayuda, siempre que exista en el
     * XHTML la etiqueta '<p:message>' con el identificador 'helpMessage'.
     *
     * @param message Mensaje que se mostrará en la zona de mensajes.
     */
    public static void showHelpMessage(String message) {
        FacesContext.getCurrentInstance().addMessage("helpMessage", new FacesMessage(FacesMessage.SEVERITY_INFO, ResourceBundle.getBundle("/Bundle").getString("Information"), message));
    }

}
