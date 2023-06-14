package fi.vm.yti.datamodel.api.v2.dto;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang3.builder.ToStringBuilder;

public class CrosswalkInfoDTO extends ResourceInfoBaseDTO {
	
	
	private CrosswalkFormat format;
	private String aggregationKey;
	private Status status;	
	private Map<String, String> label = Map.of();
	private Map<String, String> description = Map.of();
	private Set<String> languages = Set.of();
	private UUID organization;
	private Set<FileMetadata> fileMetadata = Set.of();	
	
	private String PID;

	public String getPID() {
		return PID;
	}
	public void setPID(String pID) {
		PID = pID;
	}
	
	public CrosswalkFormat getFormat() {
		return format;
	}

	public void setFormat(CrosswalkFormat type) {
		this.format = type;
	}

	public Status getStatus() {
		return status;
	}

	public void setStatus(Status status) {
		this.status = status;
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

	public Set<String> getLanguages() {
		return languages;
	}

	public void setLanguages(Set<String> languages) {
		this.languages = languages;
	}

	public UUID getOrganization() {
		return organization;
	}

	public void setOrganization(UUID organization) {
		this.organization = organization;
	}
	
    public String getAggregationKey() {
		return aggregationKey;
	}

	public void setAggregationKey(String aggregationKey) {
		this.aggregationKey = aggregationKey;
	}
	
	public Set<FileMetadata> getFileMetadata() {
		return fileMetadata;
	}

	public void setFileMetadata(Set<FileMetadata> fileMetadata) {
		this.fileMetadata = fileMetadata;
	}		

	@Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }	
}
