package es.us.lsi.hermes.kafka;

import com.google.gson.Gson;

import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.*;

public class Event {

    private String eventId;
    private String sourceId;
    private String syntax;
    private String timestamp;
    private String applicationId;
    private String eventType;
    private Map<String, Object> body;
    private Map<String, String> extraHeaders;
    private static Charset charsetUTF8 = Charset.forName("UTF-8");
    private static SimpleDateFormat rfc3339Format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");

    public Event(String eventId, String sourceId, String syntax, String applicationId, String eventType, Map<String, Object> body) {
        this.eventId = eventId;
        this.sourceId = sourceId;
        this.syntax = syntax;
        this.applicationId = applicationId;
        this.eventType = eventType;
        this.body = body;
        this.extraHeaders = new LinkedHashMap<>();
        this.timestamp = createTimestamp();
    }

    public Event(String sourceId, String syntax, String applicationId, String eventType, Map<String, Object> body) {
        this(createUUID(), sourceId, syntax, applicationId, eventType, body);
    }

    public Event(String sourceId, String syntax, String applicationId, Map<String, Object> body) {
        this(createUUID(), sourceId, syntax, applicationId, null, body);
    }

    public Event(String sourceId, String syntax, String applicationId, String eventType) {
        this(createUUID(), sourceId, syntax, applicationId, eventType, null);
    }

    public Event(String sourceId, String syntax, String applicationId) {
        this(createUUID(), sourceId, syntax, applicationId, null, null);
    }

    public String getEventId() {
        return eventId;
    }

    public String getSourceId() {
        return sourceId;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public Map<String, Object> getBody() {
        return body;
    }

    public void setBody(Map<String, Object> body) {
        this.body = body;
    }

    public void setBody(String bodyAsString) {
        body = new LinkedHashMap<>();
        body.put("value", bodyAsString);
    }

    public void setExtraHeader(String name, String value) {
        this.extraHeaders.put(name, value);
    }

    public byte[] serialize() {
        StringBuffer buffer = new StringBuffer();
        serializeHeader(buffer, "Event-Id", eventId);
        serializeHeader(buffer, "Source-Id", sourceId);
        serializeHeader(buffer, "Syntax", syntax);
        if(applicationId != null) {
            serializeHeader(buffer, "Application-Id", applicationId);
        }

        if(eventType != null) {
            serializeHeader(buffer, "Event-Type", eventType);
        }

        if(timestamp != null) {
            serializeHeader(buffer, "Timestamp", timestamp);
        }

        for (Map.Entry<String, String> entry : extraHeaders.entrySet()) {
            serializeHeader(buffer, entry.getKey(), entry.getValue());
        }

        byte[] bodyAsBytes;
        if(this.syntax.equals("application/json")) {
            bodyAsBytes = bodyAsJSON();
        } else {
            bodyAsBytes = body.get("value").toString().getBytes(charsetUTF8);
        }

        serializeHeader(buffer, "Body-Length", String.valueOf(bodyAsBytes.length));
        buffer.append("\r\n");
        byte[] headers = buffer.toString().getBytes(charsetUTF8);
        return concatenate(headers, bodyAsBytes);
    }

    public Map<String, Object> toMap() {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("Event-Id", this.eventId);
        data.put("Source-Id", this.sourceId);
        data.put("Syntax", this.syntax);
        if(this.applicationId != null) {
            data.put("Application-Id", this.applicationId);
        }

        if(this.eventType != null) {
            data.put("Event-Type", this.eventType);
        }

        if(this.timestamp != null) {
            data.put("Timestamp", this.timestamp);
        }

        data.putAll(extraHeaders);
        if(syntax.equals("application/json")) {
            data.put("Body", body);
        } else if(body.get("value") != null) {
            data.put("Body", body.get("value").toString());
        } else {
            data.put("Body", "");
        }

        return data;
    }

    private static String createUUID() {
        return UUID.randomUUID().toString();
    }

    private static void serializeHeader(StringBuffer buffer, String name, String value) {
        buffer.append(name);
        buffer.append(": ");
        buffer.append(value);
        buffer.append("\r\n");
    }

    private static String createTimestamp() {
        return rfc3339Format.format(new Date());
    }

    private static byte[] concatenate(byte[] first, byte[] second) {
        byte[] dest = new byte[first.length + second.length];
        System.arraycopy(first, 0, dest, 0, first.length);
        System.arraycopy(second, 0, dest, first.length, second.length);
        return dest;
    }

    private byte[] bodyAsJSON() {
        Gson gson = new Gson();
        return gson.toJson(this.body).getBytes(charsetUTF8);
    }
}