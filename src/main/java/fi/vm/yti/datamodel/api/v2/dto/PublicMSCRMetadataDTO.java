package fi.vm.yti.datamodel.api.v2.dto;

import java.util.List;

public class PublicMSCRMetadataDTO {

	/* read-only */
	private String dateSubmitted;
	private String modified;
	private String source;
	private String downloadUrl;
	private String previousVersion;
	
	/* modifiable */
	private String status;
	private String visibility;
	private String versionLabel;
	private String contactPoint;
	private String title;
	private String description;
	private String language;
	private String domain;
	private List<String> creator;
	private List<String> contributor;
	private List<String> identifier;
	private String issued;
	private String license;
	private String publisher;
	private List<String> relation;
	
	private String format;
	private String type;
	private List<String> keyword;
	
	
	
	public String getFormat() {
		return format;
	}
	public void setFormat(String value) {
		this.format = value;
	}
	public String getVisibility() {
		return visibility;
	}
	public void setVisibility(String visibility) {
		this.visibility = visibility;
	}
	public String getDateSubmitted() {
		return dateSubmitted;
	}
	public void setDateSubmitted(String dateSubmitted) {
		this.dateSubmitted = dateSubmitted;
	}
	public String getModified() {
		return modified;
	}
	public void setModified(String modified) {
		this.modified = modified;
	}
	public String getSource() {
		return source;
	}
	public void setSource(String source) {
		this.source = source;
	}
	public String getDownloadUrl() {
		return downloadUrl;
	}
	public void setDownloadUrl(String downloadUrl) {
		this.downloadUrl = downloadUrl;
	}
	public String getPreviousVersion() {
		return previousVersion;
	}
	public void setPreviousVersion(String previousVersion) {
		this.previousVersion = previousVersion;
	}
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}
	public String getVersionLabel() {
		return versionLabel;
	}
	public void setVersionLabel(String versionLabel) {
		this.versionLabel = versionLabel;
	}
	public String getContactPoint() {
		return contactPoint;
	}
	public void setContactPoint(String contactPoint) {
		this.contactPoint = contactPoint;
	}
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}
	public String getLanguage() {
		return language;
	}
	public void setLanguage(String language) {
		this.language = language;
	}
	public String getDomain() {
		return domain;
	}
	public void setDomain(String domain) {
		this.domain = domain;
	}
	public List<String> getCreator() {
		return creator;
	}
	public void setCreator(List<String> creator) {
		this.creator = creator;
	}
	public List<String> getContributor() {
		return contributor;
	}
	public void setContributor(List<String> contributor) {
		this.contributor = contributor;
	}
	public List<String> getIdentifier() {
		return identifier;
	}
	public void setIdentifier(List<String> identifier) {
		this.identifier = identifier;
	}
	public String getIssued() {
		return issued;
	}
	public void setIssued(String issued) {
		this.issued = issued;
	}
	public String getLicense() {
		return license;
	}
	public void setLicense(String license) {
		this.license = license;
	}
	public String getPublisher() {
		return publisher;
	}
	public void setPublisher(String publisher) {
		this.publisher = publisher;
	}
	public List<String> getRelation() {
		return relation;
	}
	public void setRelation(List<String> relation) {
		this.relation = relation;
	}

	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public List<String> getKeyword() {
		return keyword;
	}
	public void setKeyword(List<String> keyword) {
		this.keyword = keyword;
	}

	
	
}
