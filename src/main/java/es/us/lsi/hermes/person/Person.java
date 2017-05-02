package es.us.lsi.hermes.person;

import java.io.Serializable;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;

public class Person implements Serializable {

    private static final long serialVersionUID = 1L;

    private String email;
    private String fullName;
    private Integer personId;
    private String sha;

    public Person() {
        super();
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getSha() {
        if (sha == null || sha.length() == 0) {
            sha = new String(Hex.encodeHex(DigestUtils.sha256(email)));
        }

        return sha;
    }

    public void setSha(String sha) {
        this.sha = sha;
    }

    public String getFullName() {
        return fullName;
    }

    public void setFullName(String fullName) {
        this.fullName = fullName;
    }

    public Integer getPersonId() {
        return personId;
    }

    public void setPersonId(Integer personId) {
        this.personId = personId;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("[");
        sb.append(this.getFullName());
        sb.append("]");

        return sb.toString();
    }
}
