package es.us.lsi.hermes.util;

import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;
import javax.mail.*;
import javax.mail.internet.*;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Email {

    private static final Logger LOG = Logger.getLogger(Email.class.getName());

    private static Properties mailServerProperties;
    private static Session mailSession;

    static {
        LOG.log(Level.INFO, "init() - Inicialización del gestor de correo");

        mailServerProperties = Util.initProperties("Email.properties", LOG);

        mailSession = Session.getDefaultInstance(mailServerProperties, null);
    }

    public static void generateAndSendEmail(String recipient, String subject, String body) throws AddressException, MessagingException {
        generateAndSendEmail(recipient, subject, body, new ArrayList());
    }

    public static void generateAndSendEmail(String recipient, String subject, String body, File attachedFile) throws AddressException, MessagingException {
        List<File> attachedFiles = new ArrayList<>();
        attachedFiles.add(attachedFile);
        generateAndSendEmail(recipient, subject, body, attachedFiles);
    }

    public static void generateAndSendEmail(String recipient, String subject, String body, List<File> attachedFiles) throws AddressException, MessagingException {
        LOG.log(Level.INFO, "generateAndSendEmail() - Generación y envío del correo a: {0}", recipient);

        Message generateMailMessage = new MimeMessage(mailSession);
        generateMailMessage.addRecipient(Message.RecipientType.TO, new InternetAddress(recipient));

        // Asunto.
        generateMailMessage.setSubject(subject);

        // Cuerpo.
        MimeBodyPart messageBodyPart = new MimeBodyPart();
        messageBodyPart.setContent(body, "text/html; charset=utf-8");

        Multipart multipart = new MimeMultipart();
        multipart.addBodyPart(messageBodyPart);

        // Adjuntos.
        if (attachedFiles != null && !attachedFiles.isEmpty()) {
            messageBodyPart = new MimeBodyPart();
            for (File f : attachedFiles) {
                DataSource source = new FileDataSource(f);
                messageBodyPart.setDataHandler(new DataHandler(source));
                messageBodyPart.setFileName(f.getName());
                multipart.addBodyPart(messageBodyPart);
            }
        }

        generateMailMessage.setContent(multipart);
        LOG.log(Level.FINE, "generateAndSendEmail() - Email generado correctamente");

        Transport transport = mailSession.getTransport("smtp");
        transport.connect((String) mailServerProperties.get("mail.smtp.host"), (String) mailServerProperties.get("mail.smtp.user"), (String) mailServerProperties.get("mail.smtp.password"));
        transport.sendMessage(generateMailMessage, generateMailMessage.getAllRecipients());
        transport.close();
        LOG.log(Level.INFO, "generateAndSendEmail() - Email enviado correctamente");
    }
}
