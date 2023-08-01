package fi.vm.yti.datamodel.api.v2.dto;

import java.util.Map;
import java.util.Set;

public class ResourceInfoBaseDTO extends ResourceCommonDTO {
    private Map<String, String> label;
    private String editorialNote;
    private Status status;
    private ConceptDTO subject;
    private String identifier;
    private Map<String, String> note;
    private String uri;
    private String curie;
    private Set<OrganizationDTO> contributor;
    private String contact;

    public Map<String, String> getLabel() {
        return label;
    }

    public void setLabel(Map<String, String> label) {
        this.label = label;
    }

    public String getEditorialNote() {
        return editorialNote;
    }

    public void setEditorialNote(String editorialNote) {
        this.editorialNote = editorialNote;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public ConceptDTO getSubject() {
        return subject;
    }

    public void setSubject(ConceptDTO subject) {
        this.subject = subject;
    }

    public String getIdentifier() {
        return identifier;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    public Map<String, String> getNote() {
        return note;
    }

    public void setNote(Map<String, String> note) {
        this.note = note;
    }

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public String getCurie() {
        return curie;
    }

    public void setCurie(String curie) {
        this.curie = curie;
    }

    public Set<OrganizationDTO> getContributor() {
        return contributor;
    }

    public void setContributor(Set<OrganizationDTO> contributor) {
        this.contributor = contributor;
    }

    public String getContact() {
        return contact;
    }

    public void setContact(String contact) {
        this.contact = contact;
    }

}
