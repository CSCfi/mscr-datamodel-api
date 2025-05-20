package fi.vm.yti.datamodel.api.v2.dto;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonProperty.Access;

public class MSCRModelDTO extends ResourceCommonDTO {

	public record Relation(String uri, String label, String target) {};

	private MSCRState state;
	private MSCRVisibility visibility = MSCRVisibility.PUBLIC;
	
	private String versionLabel;
	private String contact;

	private ModelType type;
	private MSCRSubType subType;
	private Set<String> owner;
	
    private Map<String, String> label = Map.of();
    private Map<String, String> description = Map.of();	
    
    private Set<String> languages = Set.of();    
    
    private String domain;
    private List<String> dctCreators;
    private List<String> dctContributors;

    private List<String> dctIdentifiers;
    private String dctIssued;
    private String dctLicense;
    private String schemaProject;
    private String dctPublisher;
    private List<String> dctRelations; 
    private List<String> dcatKeywords;
    
    
    
	public List<String> getDctContributors() {
		return dctContributors;
	}
	public void setDctContributors(List<String> dctContributors) {
		this.dctContributors = dctContributors;
	}

	public List<String> getDcatKeywords() {
		return dcatKeywords;
	}
	public void setDcatKeywords(List<String> dcatKeywords) {
		this.dcatKeywords = dcatKeywords;
	}
	public String getDomain() {
		return domain;
	}
	public void setDomain(String domain) {
		this.domain = domain;
	}
	public List<String> getDctCreators() {
		return dctCreators;
	}
	public void setDctCreators(List<String> dctCreators) {
		this.dctCreators = dctCreators;
	}
	public List<String> getDctIdentifiers() {
		return dctIdentifiers;
	}
	public void setDctIdentifiers(List<String> dctIdentifiers) {
		this.dctIdentifiers = dctIdentifiers;
	}
	public String getDctIssued() {
		return dctIssued;
	}
	public void setDctIssued(String dctIssued) {
		this.dctIssued = dctIssued;
	}
	public String getDctLicense() {
		return dctLicense;
	}
	public void setDctLicense(String dctLicense) {
		this.dctLicense = dctLicense;
	}
	public String getSchemaProject() {
		return schemaProject;
	}
	public void setSchemaProject(String schemaProject) {
		this.schemaProject = schemaProject;
	}
	public String getDctPublisher() {
		return dctPublisher;
	}
	public void setDctPublisher(String dctPublisher) {
		this.dctPublisher = dctPublisher;
	}
	public List<String> getDctRelations() {
		return dctRelations;
	}
	public void setDctRelations(List<String> dctRelations) {
		this.dctRelations = dctRelations;
	}
	public ModelType getType() {
		return type;
	}
	public void setType(ModelType type) {
		this.type = type;
	}
	public String getContact() {
		return contact;
	}
	public void setContact(String contact) {
		this.contact = contact;
	}
	public Set<String> getLanguages() {
		return languages;
	}
	public void setLanguages(Set<String> languages) {
		this.languages = languages;
	}
	public Map<String, String> getLabel() {
		return label;
	}
	public void setLabel(Map<String, String> label) {
		this.label = label;
	}
	public Map<String, String> getDescription() {
		return description;
	}
	public void setDescription(Map<String, String> description) {
		this.description = description;
	}

	public MSCRState getState() {
		return state;
	}
	public void setState(MSCRState state) {
		this.state = state;
	}
	public MSCRVisibility getVisibility() {
		return visibility;
	}
	public void setVisibility(MSCRVisibility visibility) {
		this.visibility = visibility;
	}
	public String getVersionLabel() {
		return versionLabel;
	}
	public void setVersionLabel(String versionLabel) {
		this.versionLabel = versionLabel;
	}
	public MSCRSubType getSubType() {
		return subType;
	}
	public void setSubType(MSCRSubType subType) {
		this.subType = subType;
	}
	public Set<String> getOwner() {
		return owner;
	}
	public void setOwner(Set<String> owner) {
		this.owner = owner;
	}	

}
